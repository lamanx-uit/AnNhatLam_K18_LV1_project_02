from concurrent.futures import ThreadPoolExecutor
import queue
import logging
from processing import preprocessing

CDLQ = queue.Queue()

def put_CDLQ_item(product_id):
    CDLQ.put(product_id)

def CDLQ_processing(product_id):
    from crawl import get_product_data
    logging.info(f"Circuit processing DLQ item: {product_id}")
    CDLQ.get()
    logging.error(f"Circuit processing DLQ item: {product_id}")
        # Here you can add your processing logic
    total_products = 0
    batch_size = 1000  # Number of products per batch
    
    all_failed_items = []
    while not CDLQ.empty():
      all_failed_items.append(CDLQ.get())
    batch_total = len(all_failed_items)

    for batches in range(0, batch_total, batch_size):
        batch_ids = all_failed_items[batches:batches + batch_size]
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
            from main import saving
            saving(data, batches // batch_size + 1)
            logging.info(f"Batch {batches // batch_size + 1} processed and saved.")
        except Exception as e:
            logging.error(f"Error processing batch {batches // batch_size + 1}: {e}")
    return None