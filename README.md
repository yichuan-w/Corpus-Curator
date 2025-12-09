# Wikipedia Screenshot Tool

This directory contains tools for crawling and processing Wikipedia data.

## Initial Setup

To take screenshots of Wikipedia pages, you'll need to install Playwright:

```bash
pip install playwright
playwright install chromium
```

## Usage

### Basic Test

Run the test script to take screenshots of a few example Wikipedia articles:

```bash
python crawl-data.py
```

This will create a `screenshots/` directory with PNG images of the Wikipedia pages.

### Using as a Module

```python
from crawl_data import screenshot_wikipedia_article
import asyncio

# Take a screenshot of a Wikipedia article
asyncio.run(screenshot_wikipedia_article("Python (programming language)"))
```

### Custom Usage

```python
import asyncio
from pathlib import Path
from crawl_data import screenshot_wikipedia_article

async def main():
    articles = ["Machine learning", "Artificial intelligence", "Deep learning"]
    for article in articles:
        await screenshot_wikipedia_article(article, output_dir=Path("my_screenshots"))

asyncio.run(main())
```

## Next Steps

Once screenshot functionality is working, you can:
1. Download Wikipedia dumps from https://dumps.wikimedia.org/enwiki/
2. Extract article titles using WikiExtractor
3. Batch process screenshots for multiple articles

