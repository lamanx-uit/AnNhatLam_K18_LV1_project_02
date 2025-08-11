from circuitbreaker import circuit
from dlq import CDLQ, put_CDLQ_item, CDLQ_processing
import logging
import json
from datetime import datetime

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
    'tmp': BASE_DIR / 'tests' / 'tmp'
}

def fallback(product_id, selected_data, batch_number):
    # System failing
    logging.warning(f"Circuit OPEN - system failing")
    
    # Saves the current work to tmp
    # 1. Successful responds go to tmp
    tmp_file = FILES['tmp'] / f"success_batch_{batch_number}.json"
    with open(tmp_file, 'w', encoding='utf-8', errors='ignore') as f:
        json.dump(selected_data, f, ensure_ascii=False, indent=4)
    logging.info(f"Saved successful batch {len(selected_data)} to {tmp_file}")

    # 2. All unprocessed batch items to CDLQ
    put_CDLQ_item(product_id)
    logging.info(f"Queued items to CDLQ for retry")
    # Process in-processing batch/items
    
    pass
  