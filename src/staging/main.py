from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from crawl import read_data, get_product_data, setup_logging
from processing import preprocessing
from concurrent.futures import ThreadPoolExecutor
import logging
from checkpoint import stateMachine, save_checkpoint, load_checkpoint, clear_checkpoint

# Move to yaml (gotta learn)
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
FILES = {
    'products': BASE_DIR / 'data' / 'input' / 'products-0-200000.xlsx',
    
    'logs': BASE_DIR / 'logs' / 'crawl.log',
    # 'logs': BASE_DIR / 'tests' / 'logs' / 'crawl.log',
    
    'abnormal-id': BASE_DIR / 'logs' / 'DLQ.log',
    # 'abnormal-id': BASE_DIR / 'tests' / 'logs' / 'DLQ.log',
    
    'output': BASE_DIR / 'data' / 'output',
    # 'output': BASE_DIR / 'tests' / 'output'

    # 'tmp': BASE_DIR / 'data' / 'output',
    'tmp': BASE_DIR / 'tests' / 'output' / 'tmp',

    'checkpoint' : BASE_DIR / 'config' / 'checkpoints' / 'checkpoint.json'
}

def saving(data, batch_number):
    json_string = json.dumps(data, ensure_ascii=False, indent=4)

    filename = FILES['output'] / f"batch_{batch_number}.json"
    with open(filename, 'w', encoding='utf-8', errors='ignore') as f:
        f.write(json_string)
        
    logging.info(f"Saved batch {batch_number} to {filename}")

def get_product_data_wrapper(product_ids):
    total_products = 0
    batch_size = 1000  # Number of products per batch
    batch_total = len(product_ids)  # Total number of products to process
    
    # Load checkpoint
    state = stateMachine()
    try:
        checkpoint_data = load_checkpoint(filename=str(FILES['checkpoint']))
        logging.info(f"checkpoint_data: {checkpoint_data}")
        state.update_status(current_status="resume")
        if checkpoint_data:
            # Load config from checkpoint data
            state.current_batch = checkpoint_data.get('current_batch', 0)
            state.failed_id = checkpoint_data.get('failed_id', [])
            state.completed_batch = checkpoint_data.get('completed_batch', [])
            logging.info(f"Checkpoint loaded: {state.get_state()}")
            # Load config for batch processing
            start_batch = (state.current_batch * 1000) - 1000
        else:
            logging.info("No checkpoint found - starting fresh")
        start_batch = 0
    except Exception as e:
        logging.error(f"Error loading checkpoint: {e} - starting fresh")
        start_batch = 0

    for batches in range(start_batch, batch_total, batch_size):
        batch_ids = product_ids[batches:(batches + batch_size)]  # Slicing from checkpoint to the end of batch (1000)
        future = []
        data = []
        batch_number = batches // batch_size + 1
        
        if batch_number in state.completed_batch:
            logging.info(f"Batch {batch_number} already processed, skipping...")
            continue

        try:
            with ThreadPoolExecutor(max_workers=20) as executor:
                for product_id in batch_ids:
                    future.append(executor.submit(get_product_data, product_id))
                    
            for f in as_completed(future, timeout=900): # Set timeout to avoid infinite retry
                result = f.result()
                data.append(result)
                state.update_status(current_status="processing")

            # Filter results
            data = [d for d in data if d is not None]
            data_failed = [d for d in data if d.get('error') is not None]
            state.add_failed(data_failed)
            total_products += len(data)
            logging.info(f"Total products fetched in this batch: {len(data)} / {batch_size}")
            logging.info(f"Batch {batches // batch_size + 1} fetched with {len(data)} products.")
            
            # Process the result
            for item in data:
                preprocessing(item)
                
            state.update_status(current_status="processed")
            
            # Process the result and save it
            saving(data, batch_number)
            logging.info(f"Batch {batch_number} processed and saved.")

            state.update_batch(just_complete_batch=batch_number, current_batch=batch_number, failed_id=state.failed_id)
            try:
                save_checkpoint(state, filename=FILES['checkpoint'])
                logging.info(f"Checkpoint saved successfully.")
            except Exception as e:
                logging.error(f"Error saving checkpoint: {e}")
                continue
            
        except TimeoutError:
            logging.warning(f"Batch {batch_number} timeout")
            # Save partial data if any
            if data:
                saving(data, f"{batch_number}_partial")
            # Update checkpoint for partial completion
            state.update_batch(batch_number, batch_number, state.failed_id)
            state.update_status("timeout")
            save_checkpoint(state, filename=FILES['checkpoint'])
            break
        
        except Exception as e:
            state.update_status(current_status="Error")     
            save_checkpoint(state, filename=FILES['checkpoint'])
            logging.error(f"Error processing batch {batch_number}: {e}")
    logging.info(f"Total products collected for this batch: {total_products} / {batch_total}")

if __name__ == "__main__":
    setup_logging()
    product_ids = read_data()
    get_product_data_wrapper(product_ids)
    logging.info("All batches processed and saved.")
    clear_checkpoint(filename=FILES['checkpoint'])   
    logging.info("This job finished. Checkpoint cleared.")
