from concurrent.futures import ThreadPoolExecutor
import json
from crawl import read_data, get_product_data, setup_logging
from processing import preprocessing
from concurrent.futures import ThreadPoolExecutor
import logging
from dlq import CDLQ_processing
from circuitbreaker import circuit
import requests as req

# Move to yaml (gotta learn)
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
FILES = {
    'products': BASE_DIR / 'data' / 'input' / 'products-0-200000.xlsx',
    # 'logs': BASE_DIR / 'logs' / 'crawl.log',
    'logs': BASE_DIR / 'tests' / 'logs' / 'crawl.log',
    
    # 'abnormal-id': BASE_DIR / 'logs' / 'DLQ.log',
    'abnormal-id': BASE_DIR / 'tests' / 'logs' / 'DLQ.log',
    
    # 'output': BASE_DIR / 'data' / 'output',
    'output': BASE_DIR / 'tests' / 'output',

    # 'tmp': BASE_DIR / 'data' / 'output',
    'tmp': BASE_DIR / 'tests' / 'output' / 'tmp'
}

def saving(data, batch_number):
    json_string = json.dumps(data, ensure_ascii=False, indent=4)

    filename = FILES['output'] / f"batch_{batch_number}.json"
    with open(filename, 'w', encoding='utf-8', errors='ignore') as f:
        f.write(json_string)
        
    logging.info(f"Saved batch {batch_number} to {filename}")

def fallback(product_id, data, batch_number):
    from dlq import CDLQ, put_CDLQ_item
    # System failing
    logging.critical(f"Circuit OPEN - system failing")
    
    # Saves the current work to tmp
    # 1. Successful responds go to tmp
    tmp_file = FILES['tmp'] / f"success_batch_{batch_number}.json"
    with open(tmp_file, 'w', encoding='utf-8', errors='ignore') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logging.info(f"Saved successful batch {len(data)} to {tmp_file}")

    # 2. All unprocessed batch items to CDLQ
    put_CDLQ_item(product_id)
    logging.info(f"Queued {1000 - len(data)} items to CDLQ for retry")
    return None

@circuit(name='get_product_data_wrapper', 
         fallback_function=fallback, 
         failure_threshold=5, 
         recovery_timeout=60, 
         expected_exception=(req.Timeout, req.ConnectionError, req.RequestException))
def get_product_data_wrapper(product_ids):
    total_products = 0
    batch_size = 1000  # Number of products per batch
    batch_total = len(product_ids)  # Total number of products to process
    
    for batches in range(0, batch_total, batch_size):
        batch_ids = product_ids[batches:batches + batch_size]
        future = []  
        data = []   
        try:
            with ThreadPoolExecutor(max_workers=20) as executor:
                for product_id in batch_ids:
                    future.append(executor.submit(get_product_data, product_id))
            # Collect results
            for f in future:
                data.append(f.result())
            # Filter out None results
            data = [d for d in data if d is not None]
            total_products += len(data)
            logging.info(f"Total products fetched in this batch: {len(data)} / {batch_size}")
            logging.info(f"Batch {batches // batch_size + 1} fetched with {len(data)} products.")
            
            # Process the result
            for item in data:
                preprocessing(item)
            
            # Process the result and save it
            saving(data, batches // batch_size + 1)
            logging.info(f"Batch {batches // batch_size + 1} processed and saved.")

        # Circuit Open:  Fallback to DLQ
        except Exception as e:
            logging.error(f"Error processing batch {batches // batch_size + 1}: {e}")
            fallback(batch_ids, data, batches // batch_size + 1)
            CDLQ_processing(batch_ids)
            break

    logging.info(f"Total products collected for this batch: {total_products} / {batch_total}")

if __name__ == "__main__":
    setup_logging()
    product_ids = read_data()
    product_ids = product_ids[:2000]
    get_product_data_wrapper(product_ids)
    logging.info("All batches processed and saved.")