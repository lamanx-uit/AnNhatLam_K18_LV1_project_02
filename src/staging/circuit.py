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
    'tmp': BASE_DIR / 'tests' / 'output' / 'tmp'
}

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
  