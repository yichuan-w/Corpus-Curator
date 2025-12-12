import json
import argparse
from datasets import load_dataset
from tqdm import tqdm

def main():
    parser = argparse.ArgumentParser(description="Extract document IDs and names from Wikipedia dataset")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of documents to extract (default: all)"
    )
    args = parser.parse_args()
    
    print("Loading Wikipedia dataset...")
    # Loading the same dataset used in take_screenshot.py to ensure ID compatibility
    ds = load_dataset("MrLight/wikipedia-20240520")['train']
    
    total_records = len(ds)
    print(f"Dataset loaded. Total records: {total_records:,}")
    
    # Calculate dataset size
    total_size = 0
    print("Calculating dataset size...")
    sample_size = min(1000, total_records)
    for doc in tqdm(ds, desc="Calculating size", total=sample_size):
        # Estimate size: ID + title + text
        doc_size = len(str(doc.get('id', ''))) + len(str(doc.get('title', ''))) + len(str(doc.get('text', '')))
        total_size += doc_size
        if total_records > 1000 and total_size >= sample_size:
            # Sample first 1000 to estimate
            break
    
    if total_records > 1000:
        # Extrapolate from sample
        avg_size = total_size / sample_size
        estimated_total_size = avg_size * total_records
        print(f"Estimated dataset size: {estimated_total_size / (1024**3):.2f} GB (based on sample)")
    else:
        print(f"Dataset size: {total_size / (1024**3):.2f} GB")
    
    limit = args.limit
    if limit is None:
        limit = total_records
        limit_suffix = "all"
    else:
        limit_suffix = str(limit)
    
    output_file = f"doc_ids-limit{limit_suffix}.txt"
    output_file_with_names = f"doc_ids_with_names-limit{limit_suffix}.txt"
    
    print(f"\nExtracting {limit if limit < total_records else 'all'} IDs to {output_file}...")
    print(f"Extracting {limit if limit < total_records else 'all'} IDs with names to {output_file_with_names}...")
    
    count = 0
    with open(output_file, "w") as f, open(output_file_with_names, "w", encoding='utf-8') as f_names:
        for i, doc in enumerate(tqdm(ds, desc="Extracting", total=limit if limit < total_records else None)):
            if limit and i >= limit:
                break
            # Ensure we are writing the ID as a string, stripped of whitespace
            doc_id = str(doc['id']).strip()
            doc_title = doc.get('title', '').strip()
            
            # Write to doc_ids.txt (ID only)
            f.write(f"{doc_id}\n")
            
            # Write to doc_ids_with_names.txt (ID and title)
            f_names.write(f"{doc_id}\t{doc_title}\n")
            
            count += 1
            
    print(f"\nDone!")
    print(f"Created {output_file} with {count} document IDs")
    print(f"Created {output_file_with_names} with {count} document IDs and names")

if __name__ == "__main__":
    main()

