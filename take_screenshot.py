from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image
import os
from datasets import load_dataset
from tqdm import tqdm
from multiprocessing import Pool, cpu_count, Manager
import time
import fcntl

# Function to set up the Chrome WebDriver
def setup_driver(shared_driver_path=None):
    # Use file lock to ensure only one process downloads ChromeDriver
    lock_file_path = "/tmp/chromedriver_download.lock"
    
    if shared_driver_path is not None and shared_driver_path.value:
        driver_path = shared_driver_path.value
    else:
        # Use file lock to prevent concurrent downloads
        lock_file = None
        try:
            lock_file = open(lock_file_path, 'w')
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)  # Exclusive lock
            
            # Download ChromeDriver (only happens once)
            driver_path = ChromeDriverManager().install()
            
            # Store in shared variable if provided
            if shared_driver_path is not None:
                shared_driver_path.value = driver_path
            
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)  # Release lock
            lock_file.close()
        except Exception as e:
            # If lock fails, try to download anyway (might be cached)
            if lock_file:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                except:
                    pass
            driver_path = ChromeDriverManager().install()
            if shared_driver_path is not None:
                shared_driver_path.value = driver_path
    
    service = Service(driver_path)
    options = webdriver.ChromeOptions()
    
    # Find Chrome binary path
    import shutil
    chrome_binary = None
    for chrome_path in ['/usr/bin/google-chrome-stable', '/usr/bin/google-chrome', '/usr/bin/chromium-browser', '/usr/bin/chromium']:
        if os.path.exists(chrome_path):
            chrome_binary = chrome_path
            break
    
    if chrome_binary:
        options.binary_location = chrome_binary
    
    options.add_argument('--headless')  # Run in headless mode (no GUI)
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_argument('--window-size=980,980')  # Set the window size
    options.add_argument('--no-sandbox')  # Required for running in some environments
    options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# Function to capture screenshot with retry mechanism
def capture_screenshot(url_docid, driver, lock):
    url, doc_id = url_docid
    screenshot_dir = "screenshots"
    os.makedirs(screenshot_dir, exist_ok=True)
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            driver.get(url)
            if "Our servers are currently under maintenance or experiencing" in driver.page_source:
                raise Exception("Error page loaded")
            
            screenshot_path = os.path.join(screenshot_dir, f"{doc_id}.png")
            driver.save_screenshot(screenshot_path)
            
            with Image.open(screenshot_path) as img:
                img.save(screenshot_path)
            
            break  # Exit loop if successful
        except Exception as e:
            if attempt < max_retries - 1:
                with lock:
                    print(f"Retry {attempt + 1} for {url} due to {e}")
                time.sleep(5)
            else:
                with lock:
                    print(f"Failed to capture screenshot for {url} after {max_retries} attempts: {e}")

# Load dataset and create mappings
ds = load_dataset("MrLight/wikipedia-20240520")['train']
docid_to_idx = {docid: idx for idx, docid in enumerate(tqdm(ds["id"]))}

# Load existing screenshot ids
existing_files = os.listdir("screenshots")
existing_doc_ids = set()
for file in existing_files:
    if file.endswith(".png"):
        existing_doc_ids.add(file.replace(".png", ""))

# Load target document IDs
target_doc_ids = set()
with open("doc_ids.txt", "r") as f:
    for line in tqdm(f):
        target_doc_ids.add(line.strip())

target_doc_ids = target_doc_ids - existing_doc_ids
target_doc_ids = list(target_doc_ids)

# Create URLs for the target document IDs
urls = [(f"https://en.wikipedia.org/wiki/{ds[docid_to_idx[doc_id]]['title']}", doc_id) for doc_id in target_doc_ids]

# Initialize the lock and driver pool
manager = Manager()
lock = manager.Lock()
shared_driver_path = manager.Value('s', '')  # Shared string value for driver path
num_drivers = 5  # Number of parallel drivers to use

def worker_task(url_docid):
    capture_screenshot(url_docid, driver, lock)

# Run the capture_screenshot function in parallel
if __name__ == "__main__":
    # Pre-download ChromeDriver in main process to avoid concurrent download issues
    print("Pre-downloading ChromeDriver...")
    initial_driver_path = ChromeDriverManager().install()
    shared_driver_path.value = initial_driver_path
    print(f"ChromeDriver ready at: {initial_driver_path}")
    
    # Create worker initializer with shared path
    def init_worker():
        global driver
        driver = setup_driver(shared_driver_path)
    
    with Pool(num_drivers, initializer=init_worker) as pool:
        list(tqdm(pool.imap_unordered(worker_task, urls), total=len(urls)))

    # Cleanup: close the driver pool
    # Note: drivers are closed automatically when pool closes
