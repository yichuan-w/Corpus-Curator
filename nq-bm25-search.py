from datasets import load_dataset
from tqdm import tqdm
from pyserini.search import LuceneSearcher
from tqdm import tqdm

nq = load_dataset("Tevatron/wikipedia-nq")

queries = []
for example in nq['train']:
    queries.append(example['query'])

for example in nq['dev']:
    queries.append(example['query'])

for example in nq['test']:
    queries.append(example['query'])

queries_ids = []
for example in nq['train']:
    queries_ids.append(f'train_{example["query_id"]}')

for example in nq['dev']:
    queries_ids.append(f'dev_{example["query_id"]}')

for example in nq['test']:
    queries_ids.append(f'{example["query_id"]}')

from pyserini.search.lucene import LuceneSearcher

searcher = LuceneSearcher("nq-ss-text-pyserini-index")

results = searcher.batch_search(queries, queries_ids, 50, 10)

doc_ids = set()
with open('runs/run.bm25-train.trec', 'w') as f_train, open('runs/run.bm25-test.trec', 'w') as f_test, open('runs/run.bm25-dev.trec', 'w') as f_dev:
    for id_, hits in results.items():
        for i, hit in enumerate(hits):
            if id_.startswith('train'):
                f_train.write(f'{id_} Q0 {hit.docid} {i+1} {hit.score} bm25\n')
            elif id_.startswith('dev'):
                f_dev.write(f'{id_} Q0 {hit.docid} {i+1} {hit.score} bm25\n')
            else:
                f_test.write(f'{id_} Q0 {hit.docid} {i+1} {hit.score} bm25\n')
            doc_ids.add(hit.docid)

# print(f"Number of unique documents retrieved: {len(doc_ids)}")
# with open("runs/retrieved_doc_ids_2024_short_top50_with_answer.txt", "w") as f:
#     for doc_id in doc_ids:
#         f.write(f"{doc_id}\n")