from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image
import os
import sys
import urllib.parse
import time

def setup_driver(window_width=980, window_height=2000):
    """Set up Chrome WebDriver"""
    driver_path = ChromeDriverManager().install()
    service = Service(driver_path)
    options = webdriver.ChromeOptions()
    
    # Find Chrome binary path
    chrome_binary = None
    for chrome_path in ['/usr/bin/google-chrome-stable', '/usr/bin/google-chrome', '/usr/bin/chromium-browser', '/usr/bin/chromium']:
        if os.path.exists(chrome_path):
            chrome_binary = chrome_path
            break
    
    if chrome_binary:
        options.binary_location = chrome_binary
    
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument(f'--window-size={window_width},{window_height}')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def title_to_wiki_url(title):
    """
    Convert a Wikipedia article title to Wikipedia URL format.
    Example: "IEEE Frank Rosenblatt Award" -> "IEEE_Frank_Rosenblatt_Award"
    """
    # Replace spaces with underscores
    url_title = title.replace(' ', '_')
    # URL encode special characters
    url_title = urllib.parse.quote(url_title, safe='_')
    return f"https://en.wikipedia.org/wiki/{url_title}"

def capture_full_page_screenshot(driver, output_path):
    """
    Capture full page screenshot using JavaScript.
    This method captures the entire page, including parts that require scrolling.
    """
    # Get page dimensions
    total_width = driver.execute_script("return document.body.scrollWidth")
    total_height = driver.execute_script("return document.body.scrollHeight")
    
    # Set window size to full page dimensions
    driver.set_window_size(total_width, total_height)
    time.sleep(1)  # Wait for resize
    
    # Take screenshot
    driver.save_screenshot(output_path)
    
    return total_width, total_height

def capture_wiki_screenshot(title, output_dir="screenshots", output_filename=None, 
                           full_page=False, window_height=2000):
    """
    Capture a screenshot of a Wikipedia article.
    
    Args:
        title: Wikipedia article title (e.g., "IEEE Frank Rosenblatt Award")
        output_dir: Directory to save screenshot (default: "screenshots")
        output_filename: Optional custom filename (without extension). 
                        If None, uses sanitized title.
        full_page: If True, capture the entire page (including parts that require scrolling).
                   If False, capture only visible area with specified window_height.
        window_height: Height of browser window in pixels (default: 2000).
                      Only used when full_page=False.
    
    Returns:
        Path to saved screenshot
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate URL
    url = title_to_wiki_url(title)
    print(f"Title: {title}")
    print(f"URL: {url}")
    
    # Generate output filename
    if output_filename is None:
        # Sanitize title for filename (replace spaces and special chars with underscores)
        safe_filename = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in title)
        safe_filename = safe_filename.replace(' ', '_')
        output_filename = safe_filename
    
    screenshot_path = os.path.join(output_dir, f"{output_filename}.png")
    
    # Setup driver
    driver = None
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            if full_page:
                # For full page, start with a reasonable size, will resize later
                driver = setup_driver(window_width=1200, window_height=2000)
            else:
                driver = setup_driver(window_width=980, window_height=window_height)
            
            print(f"Loading page (attempt {attempt + 1}/{max_retries})...")
            
            driver.get(url)
            time.sleep(3)  # Wait for page to load
            
            # Check for error pages
            if "Our servers are currently under maintenance" in driver.page_source:
                raise Exception("Wikipedia maintenance page detected")
            
            # Take screenshot
            if full_page:
                print(f"Capturing full page screenshot...")
                width, height = capture_full_page_screenshot(driver, screenshot_path)
                print(f"Page dimensions: {width}x{height}px")
            else:
                print(f"Capturing screenshot (window height: {window_height}px)...")
                driver.save_screenshot(screenshot_path)
            
            # Optimize image
            with Image.open(screenshot_path) as img:
                img.save(screenshot_path)
            
            file_size = os.path.getsize(screenshot_path) / 1024  # KB
            print(f"Screenshot saved to: {screenshot_path} ({file_size:.1f} KB)")
            return screenshot_path
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Error (attempt {attempt + 1}): {e}. Retrying...")
                time.sleep(2)
            else:
                print(f"Failed after {max_retries} attempts: {e}")
                raise
        finally:
            if driver:
                driver.quit()
    
    return screenshot_path

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Capture Wikipedia article screenshots')
    parser.add_argument('title', help='Wikipedia article title')
    parser.add_argument('-o', '--output', dest='output_filename', 
                       help='Output filename (without extension)')
    parser.add_argument('--full-page', action='store_true',
                       help='Capture the entire page (including parts that require scrolling)')
    parser.add_argument('--height', type=int, default=2000,
                       help='Window height in pixels (default: 2000, only used when --full-page is not set)')
    
    args = parser.parse_args()
    
    try:
        screenshot_path = capture_wiki_screenshot(
            args.title, 
            output_filename=args.output_filename,
            full_page=args.full_page,
            window_height=args.height
        )
        print(f"\n✓ Success! Screenshot saved to: {screenshot_path}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)

