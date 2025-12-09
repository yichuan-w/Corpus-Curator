from datasets import load_dataset
from tqdm import tqdm
import json

dataset = load_dataset("wikipedia", language="en", date="20240520", num_proc=10)['train']
# print(dataset["train"][0])


with open("wikipedia-doc-2024.jsonl", "w") as f:
    for doc in tqdm(dataset):
        title = doc["title"]
        text = " ".join(doc["text"].split())
        id_ = doc["id"]
        f.write(json.dumps({"id": id_, "contents": f'{title}\n{text}'}) + "\n")

# dataset.save_to_disk("wikipedia-20240520", max_shard_size="1GB")

# # upload to huggingface
# dataset.push_to_hub("MrLight/wikipedia-20240520", private=True)