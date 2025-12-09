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

# Function to set up the Chrome WebDriver
def setup_driver():
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode (no GUI)
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_argument('--window-size=980,980')  # Set the window size
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
num_drivers = 5  # Number of parallel drivers to use

def worker_init():
    global driver
    driver = setup_driver()

def worker_task(url_docid):
    capture_screenshot(url_docid, driver, lock)

# Run the capture_screenshot function in parallel
if __name__ == "__main__":
    with Pool(num_drivers, initializer=worker_init) as pool:
        list(tqdm(pool.imap_unordered(worker_task, urls), total=len(urls)))

    # Cleanup: close the driver pool
    for driver in pool._pool:
        driver.quit()
