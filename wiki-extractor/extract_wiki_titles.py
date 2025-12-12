#!/usr/bin/env python3
"""
Extract titles, IDs, and optionally text from Wikipedia dumps.

This script directly parses the XML dump file to extract titles and IDs,
and optionally article text content. This is faster than using external
tools like WikiExtractor and uses less disk space.

By default, this script filters out:
- Redirect pages (aliases that point to other pages)
- Non-main namespace pages (Template:, Category:, File:, Help:, etc.)
- Disambiguation pages (pages ending with "(disambiguation)")

This matches the standard Wikipedia datasets (e.g., on HuggingFace) which typically
contain around 6M articles instead of all pages.

Usage:
    # Extract only titles and IDs (fast, low memory)
    python extract_wiki_titles.py [--date YYYYMMDD] [--output OUTPUT.jsonl]
    
    # Extract titles, IDs, and raw text content (contains wiki markup)
    python extract_wiki_titles.py --include-text [--date YYYYMMDD] [--output OUTPUT.jsonl]
    
    # Extract titles, IDs, and cleaned text (using WikiExtractor, no wiki markup)
    python extract_wiki_titles.py --include-text --use-wikiextractor [--date YYYYMMDD] [--output OUTPUT.jsonl]
    
    # Include disambiguation pages
    python extract_wiki_titles.py --include-disambiguation
"""

import xml.etree.ElementTree as ET
import bz2
import argparse
import json
import urllib.request
import subprocess
import sys
from pathlib import Path
from tqdm import tqdm

def download_dump(date: str, output_dir: Path = Path(".")) -> Path:
    """
    Download Wikipedia dump file for the given date.
    
    Args:
        date: Date in YYYYMMDD format (e.g., "20240501")
        output_dir: Directory to save the dump file
        
    Returns:
        Path to the downloaded dump file
    """
    base_url = "https://dumps.wikimedia.org/enwiki"
    dump_filename = f"enwiki-{date}-pages-articles.xml.bz2"
    dump_url = f"{base_url}/{date}/{dump_filename}"
    
    dump_path = output_dir / dump_filename
    
    if dump_path.exists():
        print(f"Dump file already exists: {dump_path}")
        return dump_path
    
    print(f"Downloading Wikipedia dump from {dump_url}...")
    print("This may take a while as dump files are large (several GB)...")
    
    def show_progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        percent = min(downloaded * 100 / total_size, 100)
        print(f"\rDownloaded: {percent:.1f}% ({downloaded / (1024*1024):.1f} MB)", end="")
    
    try:
        urllib.request.urlretrieve(dump_url, dump_path, show_progress)
        print(f"\nDownload complete: {dump_path}")
        return dump_path
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise FileNotFoundError(
                f"Dump file not found for date {date}. "
                f"Available dates can be checked at: https://dumps.wikimedia.org/enwiki/"
            )
        raise

def is_valid_article(title: str, is_redirect: bool = False, filter_disambiguation: bool = True) -> bool:
    """
    Check if a page is a valid article (not redirect, not namespace page, etc.)
    
    Args:
        title: Page title
        is_redirect: Whether this page is a redirect
        filter_disambiguation: Whether to filter disambiguation pages
        
    Returns:
        True if this is a valid article page
    """
    # Skip redirects (aliases)
    if is_redirect:
        return False
    
    # Skip non-main namespace pages
    # Main namespace has no prefix, other namespaces have prefixes like:
    # Template:, Category:, File:, Help:, Wikipedia:, Portal:, etc.
    if ':' in title:
        # Check if it's a namespace prefix (not a colon in the middle of title)
        parts = title.split(':', 1)
        namespace = parts[0]
        # Common namespaces to exclude
        excluded_namespaces = {
            'Template', 'Category', 'File', 'Image', 'Help', 'Wikipedia',
            'Portal', 'Book', 'Draft', 'User', 'MediaWiki', 'Module',
            'Media', 'Special', 'Talk', 'User talk', 'Wikipedia talk',
            'File talk', 'Template talk', 'Category talk', 'Help talk',
            'Portal talk', 'Book talk', 'Draft talk', 'Module talk'
        }
        if namespace in excluded_namespaces:
            return False
    
    # Optionally filter disambiguation pages
    if filter_disambiguation and title.endswith('(disambiguation)'):
        return False
    
    return True

def check_wikiextractor():
    """Check if WikiExtractor is installed."""
    try:
        import wikiextractor
        return True
    except ImportError:
        try:
            result = subprocess.run(
                [sys.executable, "-m", "wikiextractor.WikiExtractor", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

def extract_with_wikiextractor(dump_path: Path, output_path: Path, limit: int = None, filter_disambiguation: bool = True):
    """
    Extract titles, IDs, and cleaned text using WikiExtractor.
    
    WikiExtractor outputs cleaned text (without wiki markup), which is what
    most applications need.
    
    Args:
        dump_path: Path to the Wikipedia dump XML file (can be .bz2 compressed)
        output_path: Path to output JSONL file
        limit: Maximum number of titles to extract (None for all)
        filter_disambiguation: Whether to filter disambiguation pages (default: True)
    """
    if not check_wikiextractor():
        raise RuntimeError(
            "WikiExtractor is not installed. "
            "Please install it using: pip install wikiextractor"
        )
    
    print(f"Extracting titles and cleaned text using WikiExtractor from {dump_path}...")
    print("WikiExtractor will clean the text (remove wiki markup)...")
    print("Filtering: redirects, non-main namespace pages" + 
          (", disambiguation pages" if filter_disambiguation else ""))
    
    # Create temporary directory for WikiExtractor output
    import tempfile
    import shutil
    temp_dir = Path(tempfile.mkdtemp(prefix="wikiextractor_"))
    
    try:
        # Run WikiExtractor
        print("Running WikiExtractor (this may take a while)...")
        cmd = [
            sys.executable, "-m", "wikiextractor.WikiExtractor",
            str(dump_path),
            "--json",
            "--output", str(temp_dir),
            "--processes", "4",
            "--quiet"
        ]
        
        subprocess.run(cmd, check=True)
        
        # Parse WikiExtractor output and apply filters
        articles = []
        json_files = sorted(temp_dir.glob("**/wiki_*"))
        
        print(f"Found {len(json_files)} extracted files. Parsing and filtering...")
        
        for json_file in tqdm(json_files, desc="Parsing WikiExtractor output"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if limit and len(articles) >= limit:
                            break
                        
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            article = json.loads(line)
                            # WikiExtractor JSON format: {"id": "", "revid": "", "url": "", "title": "", "text": "..."}
                            title = article.get("title", "")
                            page_id = article.get("id", "")
                            
                            # Apply same filters as direct XML parsing
                            if title and page_id and is_valid_article(title, is_redirect=False, filter_disambiguation=filter_disambiguation):
                                articles.append({
                                    'id': page_id,
                                    'title': title,
                                    'text': article.get('text', '')  # Already cleaned by WikiExtractor
                                })
                        except json.JSONDecodeError:
                            continue
                        
                        if limit and len(articles) >= limit:
                            break
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                continue
            
            if limit and len(articles) >= limit:
                break
        
        # Save to JSONL
        print(f"\nSaving {len(articles)} articles to {output_path}...")
        with open(output_path, 'w', encoding='utf-8') as f:
            for article in tqdm(articles, desc="Writing"):
                f.write(json.dumps(article, ensure_ascii=False) + "\n")
        
        print(f"\nDone! Extracted {len(articles)} articles with cleaned text.")
        print(f"Output saved to: {output_path}")
        
    finally:
        # Clean up temporary directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

def extract_titles_only(dump_path: Path, output_path: Path, limit: int = None, filter_disambiguation: bool = True, include_text: bool = False):
    """
    Extract titles and IDs (and optionally text) from Wikipedia dump XML file.
    
    This function directly parses the XML without needing external tools,
    making it faster and more memory-efficient.
    Filters out redirects, non-main namespace pages, and optionally disambiguation pages.
    
    Args:
        dump_path: Path to the Wikipedia dump XML file (can be .bz2 compressed)
        output_path: Path to output JSONL file
        limit: Maximum number of titles to extract (None for all)
        filter_disambiguation: Whether to filter disambiguation pages (default: True)
        include_text: Whether to extract article text content (default: False)
    """
    print(f"Extracting {'titles and text' if include_text else 'titles'} from {dump_path}...")
    print("This will parse the XML file directly (no full extraction needed)...")
    print("Filtering: redirects, non-main namespace pages" + 
          (", disambiguation pages" if filter_disambiguation else ""))
    
    articles = []
    current_page = {}
    in_revision = False
    in_text = False
    is_redirect = False
    
    # Determine if file is compressed
    is_compressed = str(dump_path).endswith('.bz2')
    
    # Open file (compressed or not)
    if is_compressed:
        file_handle = bz2.open(dump_path, 'rt', encoding='utf-8')
    else:
        file_handle = open(dump_path, 'r', encoding='utf-8')
    
    try:
        # Use iterparse for memory-efficient parsing
        context = ET.iterparse(file_handle, events=('start', 'end'))
        
        for event, elem in tqdm(context, desc="Parsing XML"):
            if event == 'start':
                if elem.tag.endswith('page'):
                    current_page = {}
                    is_redirect = False
                elif elem.tag.endswith('revision'):
                    in_revision = True
                elif elem.tag.endswith('redirect'):
                    # This page is a redirect (alias)
                    is_redirect = True
            elif event == 'end':
                if elem.tag.endswith('title') and not in_revision:
                    current_page['title'] = elem.text or ''
                elif elem.tag.endswith('id') and not in_revision and 'id' not in current_page:
                    # Page ID (not revision ID)
                    current_page['id'] = elem.text or ''
                elif elem.tag.endswith('text') and include_text and in_revision:
                    # Text content (can be very long)
                    text = elem.text or ''
                    if text:
                        current_page['text'] = text
                    in_text = False
                elif elem.tag.endswith('revision'):
                    in_revision = False
                elif elem.tag.endswith('page'):
                    # End of page, save if we have title and id and it's a valid article
                    title = current_page.get('title', '')
                    page_id = current_page.get('id', '')
                    if title and page_id and is_valid_article(title, is_redirect, filter_disambiguation):
                        article = {
                            'id': page_id,
                            'title': title
                        }
                        if include_text and 'text' in current_page:
                            article['text'] = current_page['text']
                        articles.append(article)
                        
                        if limit and len(articles) >= limit:
                            break
                    
                    current_page = {}
                    in_revision = False
                    in_text = False
                    is_redirect = False
                    # Clear element to free memory
                    elem.clear()
                    
    except ET.ParseError as e:
        print(f"XML parsing error: {e}")
        print("Trying alternative parsing method...")
        # Fallback: simple line-by-line parsing
        file_handle.close()
        if is_compressed:
            file_handle = bz2.open(dump_path, 'rt', encoding='utf-8')
        else:
            file_handle = open(dump_path, 'r', encoding='utf-8')
        
        articles = []
        current_page = {}
        in_title = False
        in_id = False
        in_revision = False
        in_text = False
        is_redirect = False
        text_lines = []
        
        for line in tqdm(file_handle, desc="Parsing XML (line-by-line)"):
            if limit and len(articles) >= limit:
                break
                
            if '<revision>' in line:
                in_revision = True
                in_text = False
                text_lines = []
            elif '</revision>' in line:
                if include_text and text_lines:
                    current_page['text'] = ''.join(text_lines)
                in_revision = False
                in_text = False
            elif '<redirect' in line:
                # This page is a redirect (alias)
                is_redirect = True
            elif '<text' in line and include_text and in_revision:
                # Start of text tag - text may span multiple lines
                in_text = True
                # Extract text from this line if it's self-contained
                start = line.find('>') + 1
                end = line.find('</text>')
                if end > start:
                    text_lines.append(line[start:end])
                    in_text = False
                elif start < len(line):
                    text_lines.append(line[start:])
            elif in_text and include_text:
                # Collecting text content
                if '</text>' in line:
                    end = line.find('</text>')
                    if end >= 0:
                        text_lines.append(line[:end])
                        in_text = False
                else:
                    text_lines.append(line)
            elif '<title>' in line and not in_revision:
                start = line.find('<title>') + 7
                end = line.find('</title>')
                if end > start:
                    current_page['title'] = line[start:end]
            elif '<id>' in line and not in_revision and 'id' not in current_page:
                # Check if this is page id (not revision id)
                start = line.find('<id>') + 4
                end = line.find('</id>')
                if end > start:
                    current_page['id'] = line[start:end]
            elif '</page>' in line:
                title = current_page.get('title', '')
                page_id = current_page.get('id', '')
                if title and page_id and is_valid_article(title, is_redirect, filter_disambiguation):
                    article = {
                        'id': page_id,
                        'title': title
                    }
                    if include_text and 'text' in current_page:
                        article['text'] = current_page['text']
                    articles.append(article)
                current_page = {}
                in_revision = False
                in_text = False
                is_redirect = False
                text_lines = []
    
    finally:
        file_handle.close()
    
    # Save to JSONL
    print(f"\nSaving {len(articles)} titles to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        for article in tqdm(articles, desc="Writing"):
            f.write(json.dumps(article, ensure_ascii=False) + "\n")
    
    print(f"\nDone! Extracted {len(articles)} titles.")
    print(f"Output saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Extract only titles and IDs from Wikipedia dumps"
    )
    parser.add_argument(
        "--date",
        type=str,
        default="20251201",
        help="Date in YYYYMMDD format (default: 20251201)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of titles to extract (default: all)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSONL file path (default: wikipedia-titles-{date}.jsonl)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download if dump file already exists"
    )
    parser.add_argument(
        "--include-disambiguation",
        action="store_true",
        help="Include disambiguation pages (default: excluded)"
    )
    parser.add_argument(
        "--include-text",
        action="store_true",
        help="Extract article text content in addition to titles and IDs (default: False)"
    )
    parser.add_argument(
        "--use-wikiextractor",
        action="store_true",
        help="Use WikiExtractor to extract cleaned text (requires wikiextractor package). "
             "This will output cleaned text without wiki markup. "
             "If not specified, raw wiki markup text will be extracted (if --include-text is used)."
    )
    
    args = parser.parse_args()
    
    # Download dump
    dump_path = None
    if not args.skip_download:
        try:
            dump_path = download_dump(args.date)
        except FileNotFoundError as e:
            print(f"Error: {e}")
            print("\nAvailable dates can be checked at: https://dumps.wikimedia.org/enwiki/")
            return
    else:
        dump_filename = f"enwiki-{args.date}-pages-articles.xml.bz2"
        dump_path = Path(dump_filename)
        if not dump_path.exists():
            print(f"Error: Dump file not found: {dump_path}")
            return
    
    # Extract titles (and optionally text)
    if args.use_wikiextractor:
        # Use WikiExtractor for cleaned text
        if not args.include_text:
            print("Warning: --use-wikiextractor requires --include-text. Enabling --include-text automatically.")
        output_path = Path(args.output) if args.output else Path(f"wikipedia-dump-cleaned-{args.date}.jsonl")
        extract_with_wikiextractor(dump_path, output_path, limit=args.limit, filter_disambiguation=not args.include_disambiguation)
    else:
        # Direct XML parsing (faster, but text contains wiki markup if --include-text is used)
        output_path = Path(args.output) if args.output else Path(f"wikipedia-{'dump' if args.include_text else 'titles'}-{args.date}.jsonl")
        extract_titles_only(dump_path, output_path, limit=args.limit, filter_disambiguation=not args.include_disambiguation, include_text=args.include_text)

if __name__ == "__main__":
    main()

