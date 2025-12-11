from datasets import load_dataset
from tqdm import tqdm
import json

# Use wikimedia/wikipedia format: "YYYYMMDD.en"
dataset = load_dataset("wikimedia/wikipedia", "20240501.en", num_proc=10)['train']
# print(dataset["train"][0])

limit = 10  # Only output first 10 documents

with open("wikipedia-doc-2024.jsonl", "w") as f:
    for i, doc in enumerate(tqdm(dataset)):
        if i >= limit:
            break
        title = doc["title"]
        text = " ".join(doc["text"].split())
        id_ = doc["id"]
        f.write(json.dumps({"id": id_, "contents": f'{title}\n{text}'}) + "\n")

# dataset.save_to_disk("wikipedia-20240501", max_shard_size="1GB")

# # upload to huggingface
# dataset.push_to_hub("MrLight/wikipedia-20240501", private=True)