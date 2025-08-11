import queue
import logging
from crawl import get_product_data
from pathlib import Path
from processing import preprocessing
from main import saving, get_product_data_wrapper

CDLQ = queue.Queue()

def put_CDLQ_item(product_id):
    CDLQ.put(product_id)

def CDLQ_processing(product_id):
    logging.info(f"Circuit processing DLQ item: {product_id}")
    while not CDLQ.empty():
        product_id = CDLQ.get()
        logging.error(f"Circuit processing DLQ item: {product_id}")
        # Here you can add your processing logic
        get_product_data_wrapper([product_id])
    return None