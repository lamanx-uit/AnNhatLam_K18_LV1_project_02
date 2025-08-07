from concurrent.futures import ThreadPoolExecutor
import json
from crawl import read_data, setup_session, get_product_data
from processing import preprocessing
from concurrent.futures import ThreadPoolExecutor

def saving(data, batch_number):
    json_string = json.dumps(data, ensure_ascii=False, indent=4)
    
    filename = f"/home/lamanx/DEC/Lab2/tests/output/batch_{batch_number}.json"  
    with open(filename, 'w', encoding='utf-8', errors='ignore') as f:
        f.write(json_string)
        
    print(f"Saved batch {batch_number} to {filename}")

def get_product_data_wrapper(product_ids):
    total_products = 0
    batch_size = 1000  # Number of products per batch
    batch_total = len(product_ids)  # Total number of products to process

    for batches in range(0, batch_total, batch_size):
        batch_ids = product_ids[batches:batches + batch_size]
        future = []
        data = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            for product_id in batch_ids:
                future.append(executor.submit(get_product_data, product_id))
        # Collect results
        for f in future:
            data.append(f.result())
        # Filter out None results
        data = [d for d in data if d is not None]
        total_products += len(data)
        print(f"Total products fetched in this batch: {len(data)} / {batch_size}")
        print(f"Batch {batches // batch_size + 1} fetched with {len(data)} products.")
        
        # Process the result
        for item in data:
            preprocessing(item)
        
        # Process the result and save it
        saving(data, batches // batch_size + 1)
        print(f"Batch {batches // batch_size + 1} processed and saved.")

    print(f"Total products collected for this batch: {total_products} / {batch_total}")

if __name__ == "__main__":
    product_ids = read_data()
    product_ids = product_ids[:2000]
    get_product_data_wrapper(product_ids)
    print("All batches processed and saved.")