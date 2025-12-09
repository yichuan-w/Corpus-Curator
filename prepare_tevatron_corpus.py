import os
from tqdm import tqdm
import json
from datasets import load_dataset

docids = []
with open("runs/retrieved_doc_ids_2024_short_top50_with_answer.txt", "r") as f:
    for line in f:
        docids.append(line.strip())

ds = load_dataset("MrLight/wikipedia-20240520")['train']
docid_to_idx = {docid: idx for idx, docid in enumerate(tqdm(ds["id"]))}


with open('wiki-sc-final-tevatron.jsonl', 'w') as f:
    for docid in tqdm(docids):
        idx = docid_to_idx[docid]
        title = ds[idx]['title']
        text = " ".join(ds[idx]['text'].split()[:500])
        f.write(json.dumps({"docid": docid, "text": text, "title": title}) + "\n")