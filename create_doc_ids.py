import json
from datasets import load_dataset
from tqdm import tqdm

def main():
    print("Loading Wikipedia dataset...")
    # Loading the same dataset used in take_screenshot.py to ensure ID compatibility
    ds = load_dataset("MrLight/wikipedia-20240520")['train']
    
    print(f"Dataset loaded. Total records: {len(ds)}")
    
    output_file = "doc_ids.txt"
    limit = 100  # Extract first 100 IDs
    
    print(f"Extracting first {limit} IDs to {output_file}...")
    
    with open(output_file, "w") as f:
        for i, doc in enumerate(tqdm(ds)):
            if i >= limit:
                break
            # Ensure we are writing the ID as a string, stripped of whitespace
            doc_id = str(doc['id']).strip()
            f.write(f"{doc_id}\n")
            
    print("Done!")

if __name__ == "__main__":
    main()

