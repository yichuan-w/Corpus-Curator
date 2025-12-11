from datasets import load_dataset
from tqdm import tqdm
import json

ds = load_dataset("wikimedia/wikipedia", "20231101.en")['train']

limit = 10  # Only output first 10 documents

with open("wikipedia-doc.jsonl", "w") as f:
    for i, doc in enumerate(ds):
        if i >= limit:
            break
        title = doc["title"]
        text = doc["text"]
        id_ = doc["id"]
        f.write(json.dumps({"id": id_, "contents": f'{title}\n{text}'}) + "\n")
        

