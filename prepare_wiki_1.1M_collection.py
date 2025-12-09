
from datasets import load_dataset
from tqdm import tqdm
import json

# Load dataset and create mappings
ds = load_dataset("MrLight/wikipedia-20240520")['train']
docid_to_idx = {docid: idx for idx, docid in enumerate(tqdm(ds["id"]))}

# Load target document IDs
target_doc_ids = set()
with open("runs/retrieved_doc_ids_2024_short_top50_with_answer.txt", "r") as f:
    for line in tqdm(f):
        target_doc_ids.add(line.strip())

with open("wiki-2024-nq-top50-1.3M.jsonl", "w") as f:
    for doc_id in tqdm(target_doc_ids):
        docid = str(doc_id)
        idx = docid_to_idx[docid]
        title = ds[idx]['title']
        text = " ".join(ds[idx]['text'].split()[:500])
        f.write(json.dumps({"id": docid, "contents": f'{title}\n{text}'}) + "\n")