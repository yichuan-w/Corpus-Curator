from datasets import load_dataset
from tqdm import tqdm
import json

ds = load_dataset("wikimedia/wikipedia", "20231101.en")['train']

with open("wikimedia-wikipedia-20231101.jsonl", "w") as f:
    for doc in tqdm(ds):
        title = doc["title"]
        text = doc["text"]
        id_ = doc["id"]
        f.write(json.dumps({"id": id_, "contents": f'{title}\n{text}'}) + "\n")
        

