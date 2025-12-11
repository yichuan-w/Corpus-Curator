#!/usr/bin/env python3
"""
Extract only titles and IDs from Wikipedia dumps.

This script directly parses the XML dump file to extract only titles and IDs,
without needing to extract full article content. This is much faster and uses
less disk space.

Usage:
    python extract_wiki_titles.py [--date YYYYMMDD] [--output OUTPUT.jsonl]
"""

import xml.etree.ElementTree as ET
import bz2
import argparse
import json
import urllib.request
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

def extract_titles_only(dump_path: Path, output_path: Path, limit: int = None):
    """
    Extract only titles and IDs from Wikipedia dump XML file.
    
    This function directly parses the XML without extracting full content,
    making it much faster and more memory-efficient.
    
    Args:
        dump_path: Path to the Wikipedia dump XML file (can be .bz2 compressed)
        output_path: Path to output JSONL file
        limit: Maximum number of titles to extract (None for all)
    """
    print(f"Extracting titles from {dump_path}...")
    print("This will parse the XML file directly (no full extraction needed)...")
    
    articles = []
    current_page = {}
    in_revision = False
    
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
                elif elem.tag.endswith('revision'):
                    in_revision = True
            elif event == 'end':
                if elem.tag.endswith('title') and not in_revision:
                    current_page['title'] = elem.text or ''
                elif elem.tag.endswith('id') and not in_revision and 'id' not in current_page:
                    # Page ID (not revision ID)
                    current_page['id'] = elem.text or ''
                elif elem.tag.endswith('page'):
                    # End of page, save if we have title and id
                    if current_page.get('title') and current_page.get('id'):
                        articles.append({
                            'id': current_page['id'],
                            'title': current_page['title']
                        })
                        
                        if limit and len(articles) >= limit:
                            break
                    
                    current_page = {}
                    in_revision = False
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
        
        for line in tqdm(file_handle, desc="Parsing XML (line-by-line)"):
            if limit and len(articles) >= limit:
                break
                
            if '<revision>' in line:
                in_revision = True
            elif '</revision>' in line:
                in_revision = False
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
                if current_page.get('title') and current_page.get('id'):
                    articles.append({
                        'id': current_page['id'],
                        'title': current_page['title']
                    })
                current_page = {}
                in_revision = False
    
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
    
    # Extract titles
    output_path = Path(args.output) if args.output else Path(f"wikipedia-titles-{args.date}.jsonl")
    extract_titles_only(dump_path, output_path, limit=args.limit)

if __name__ == "__main__":
    main()

