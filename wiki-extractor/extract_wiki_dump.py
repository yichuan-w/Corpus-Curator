#!/usr/bin/env python3
"""
Extract articles and IDs from Wikipedia dumps using WikiExtractor.

This script:
1. Downloads a Wikipedia dump file from https://dumps.wikimedia.org/enwiki/
2. Uses WikiExtractor to extract articles in JSON format
3. Parses the output to extract article IDs and content
4. Saves to JSONL format compatible with existing code

Usage:
    python extract_wiki_dump.py [--date YYYYMMDD] [--limit N] [--output OUTPUT.jsonl]
"""

import os
import sys
import json
import subprocess
import argparse
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
    # Wikipedia dumps are available at:
    # https://dumps.wikimedia.org/enwiki/YYYYMMDD/enwiki-YYYYMMDD-pages-articles.xml.bz2
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

def check_wikiextractor():
    """Check if WikiExtractor is installed."""
    try:
        # Try importing the module directly
        import wikiextractor
        return True
    except ImportError:
        # Fallback: try running the command
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

def extract_articles(dump_path: Path, output_dir: Path = Path("extracted")) -> Path:
    """
    Extract articles from Wikipedia dump using WikiExtractor.
    
    Args:
        dump_path: Path to the Wikipedia dump file
        output_dir: Directory to save extracted files
        
    Returns:
        Path to the directory containing extracted files
    """
    output_dir.mkdir(exist_ok=True)
    
    print(f"Extracting articles from {dump_path}...")
    print("This may take a while...")
    
    # WikiExtractor command with JSON output
    # --json: output in JSON format
    # --processes: number of parallel processes
    cmd = [
        "python", "-m", "wikiextractor.WikiExtractor",
        str(dump_path),
        "--json",
        "--output", str(output_dir),
        "--processes", "4",  # Adjust based on your CPU
        "--quiet"  # Suppress progress info (we'll show our own)
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print(f"Extraction complete. Files saved to: {output_dir}")
        return output_dir
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"WikiExtractor failed: {e}")

def parse_extracted_files(extracted_dir: Path, limit: int = None) -> list:
    """
    Parse extracted JSON files and return list of articles.
    
    Args:
        extracted_dir: Directory containing extracted files
        limit: Maximum number of articles to return (None for all)
        
    Returns:
        List of dictionaries with 'id', 'title', 'text', etc.
    """
    articles = []
    json_files = sorted(extracted_dir.glob("**/wiki_*"))
    
    print(f"Found {len(json_files)} extracted files. Parsing...")
    
    for json_file in tqdm(json_files, desc="Parsing files"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if limit and len(articles) >= limit:
                        return articles
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        article = json.loads(line)
                        # WikiExtractor JSON format: {"id": "", "revid": "", "url": "", "title": "", "text": "..."}
                        if article.get("id") and article.get("title"):
                            articles.append(article)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
            continue
    
    return articles

def save_to_jsonl(articles: list, output_path: Path):
    """
    Save articles to JSONL format.
    
    Args:
        articles: List of article dictionaries
        output_path: Path to output JSONL file
    """
    print(f"Saving {len(articles)} articles to {output_path}...")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for article in tqdm(articles, desc="Writing"):
            # Format: {"id": "", "title": "", "text": ""}
            doc = {
                "id": str(article.get("id", "")),
                "title": article.get("title", ""),
                "text": article.get("text", "")
            }
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    
    print(f"Saved {len(articles)} articles to {output_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Extract articles and IDs from Wikipedia dumps using WikiExtractor"
    )
    parser.add_argument(
        "--date",
        type=str,
        default="20240501",
        help="Date in YYYYMMDD format (default: 20240501)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of articles to extract (default: all)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSONL file path (default: wikipedia-dump-{date}.jsonl)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download if dump file already exists"
    )
    parser.add_argument(
        "--skip-extraction",
        action="store_true",
        help="Skip extraction if extracted files already exist"
    )
    
    args = parser.parse_args()
    
    # Check if WikiExtractor is available
    if not check_wikiextractor():
        print("Error: WikiExtractor is not installed.")
        print("Please install it using: pip install wikiextractor")
        print("Or if using uv: uv pip install wikiextractor")
        sys.exit(1)
    
    # Download dump
    dump_path = None
    if not args.skip_download:
        try:
            dump_path = download_dump(args.date)
        except FileNotFoundError as e:
            print(f"Error: {e}")
            print("\nAvailable dates can be checked at: https://dumps.wikimedia.org/enwiki/")
            sys.exit(1)
    else:
        dump_filename = f"enwiki-{args.date}-pages-articles.xml.bz2"
        dump_path = Path(dump_filename)
        if not dump_path.exists():
            print(f"Error: Dump file not found: {dump_path}")
            sys.exit(1)
    
    # Extract articles
    extracted_dir = Path("extracted")
    if not args.skip_extraction or not extracted_dir.exists():
        extract_articles(dump_path, extracted_dir)
    else:
        print(f"Using existing extracted files in {extracted_dir}")
    
    # Parse and save
    articles = parse_extracted_files(extracted_dir, limit=args.limit)
    
    if not articles:
        print("Warning: No articles extracted!")
        sys.exit(1)
    
    output_path = Path(args.output) if args.output else Path(f"wikipedia-dump-{args.date}.jsonl")
    save_to_jsonl(articles, output_path)
    
    print(f"\nDone! Extracted {len(articles)} articles.")
    print(f"Output saved to: {output_path}")

if __name__ == "__main__":
    main()

