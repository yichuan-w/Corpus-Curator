"""
Script to take screenshots of Wikipedia pages.
This is an initial test to understand how to capture Wikipedia page screenshots.

Installation:
    pip install playwright
    playwright install chromium

Usage:
    # Test with a few example articles
    python crawl-data.py
    
    # Or use as a module:
    from crawl_data import screenshot_wikipedia_article
    import asyncio
    asyncio.run(screenshot_wikipedia_article("Python (programming language)"))
"""

import asyncio
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright, Browser, Page
except ImportError:
    raise ImportError(
        "playwright is not installed. Install it with: pip install playwright && playwright install chromium"
    )


def title_to_url(title: str, base_url: str = "https://en.wikipedia.org/wiki/") -> str:
    """Convert a Wikipedia article title to a URL."""
    # Replace spaces with underscores and URL encode
    encoded_title = title.replace(" ", "_")
    return f"{base_url}{encoded_title}"


async def take_screenshot(
    page: Page,
    url: str,
    output_path: Path,
    wait_time: int = 2000,
    viewport_width: int = 1920,
    viewport_height: int = 1080,
) -> bool:
    """
    Take a screenshot of a Wikipedia page.
    
    Args:
        page: Playwright page object
        url: URL to screenshot
        output_path: Path to save the screenshot
        wait_time: Time to wait after page load (ms)
        viewport_width: Browser viewport width
        viewport_height: Browser viewport height
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Set viewport size
        await page.set_viewport_size({"width": viewport_width, "height": viewport_height})
        
        # Navigate to the page
        await page.goto(url, wait_until="networkidle", timeout=30000)
        
        # Wait a bit for any dynamic content to load
        await page.wait_for_timeout(wait_time)
        
        # Take screenshot
        await page.screenshot(path=str(output_path), full_page=True)
        
        print(f"✓ Screenshot saved: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Error taking screenshot of {url}: {e}")
        return False


async def screenshot_wikipedia_article(
    title_or_url: str,
    output_dir: Path = Path("screenshots"),
    browser: Optional[Browser] = None,
) -> Path:
    """
    Take a screenshot of a Wikipedia article.
    
    Args:
        title_or_url: Wikipedia article title (e.g., "Python (programming language)") or full URL
        output_dir: Directory to save screenshots
        browser: Optional browser instance (if None, will create a new one)
    
    Returns:
        Path to the saved screenshot
    """
    # Determine if input is a URL or title
    if title_or_url.startswith("http"):
        url = title_or_url
        # Extract title from URL for filename
        title = url.split("/wiki/")[-1].replace("_", " ")
    else:
        title = title_or_url
        url = title_to_url(title)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create safe filename from title
    safe_filename = title.replace(" ", "_").replace("/", "_")
    safe_filename = "".join(c for c in safe_filename if c.isalnum() or c in ("_", "-", "."))
    output_path = output_dir / f"{safe_filename}.png"
    
    # Use provided browser or create a new one
    should_close_browser = browser is None
    if browser is None:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=True)
    
    try:
        page = await browser.new_page()
        success = await take_screenshot(page, url, output_path)
        await page.close()
        
        if success:
            return output_path
        else:
            raise Exception(f"Failed to take screenshot of {url}")
    finally:
        if should_close_browser:
            await browser.close()


async def main():
    """Test function to take screenshots of a few Wikipedia articles."""
    # Test with a few example articles
    test_articles = [
        "Python (programming language)",
        "Machine learning",
        "Artificial intelligence",
    ]
    
    output_dir = Path("screenshots")
    output_dir.mkdir(exist_ok=True)
    
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    
    try:
        for article in test_articles:
            print(f"\nTaking screenshot of: {article}")
            try:
                screenshot_path = await screenshot_wikipedia_article(
                    article, output_dir=output_dir, browser=browser
                )
                print(f"Success! Saved to: {screenshot_path}")
            except Exception as e:
                print(f"Failed: {e}")
    finally:
        await browser.close()
        await playwright.stop()


if __name__ == "__main__":
    # Run the test
    asyncio.run(main())

