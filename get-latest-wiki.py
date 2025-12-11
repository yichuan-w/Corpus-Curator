from datasets import load_dataset
from datasets import DownloadConfig
from tqdm import tqdm
import json

# Load Wikipedia dataset for 2025-12-01
date = "20251201"
print(f"Loading Wikipedia dataset from {date}...")

# Configure download with retries
# Note: datasets 3.6.0 doesn't support timeout parameter
download_config = DownloadConfig(
    num_proc=1,  # Use single process to avoid serialization errors
    max_retries=5,  # Retry up to 5 times on failure
    resume_download=True,  # Resume interrupted downloads
)

try:
    ds = load_dataset("wikipedia", language="en", date=date, download_config=download_config)['train']
    print(f"Successfully loaded Wikipedia dataset from {date}")
except Exception as e:
    print(f"Error loading dataset: {e}")
    print("\nNote: If download keeps timing out, consider:")
    print("1. Using wikimedia/wikipedia (already processed): load_dataset('wikimedia/wikipedia', '20231101.en')")
    print("2. Downloading dump files manually first")
    print("3. Using extract_wiki_titles.py for titles only")
    raise

with open(f"wikipedia-doc-latest-{date}.jsonl", "w") as f:
    for doc in tqdm(ds, desc="Processing"):
        title = doc["title"]
        text = doc["text"]
        id_ = doc["id"]
        f.write(json.dumps({"id": id_, "contents": f'{title}\n{text}'}) + "\n")
        

