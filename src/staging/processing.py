import html
import re
from bs4 import BeautifulSoup
from pathlib import Path
import logging

def preprocessing(data):
    text = data.get('description', '')
    data_id = data.get('id', 'unknown')

    if text:
        text = str(text)
        # Remove HTML tags and decode HTML entities
        text = html.unescape(text)
        text = BeautifulSoup(text, 'html.parser').get_text()
        text = re.sub(r'\s+', ' ', text).strip()
        
        # surrogate characters
        text = text.encode('utf-8', errors='ignore').decode('utf-8')
    else:
        text = ""
    
    data['description'] = text
    logging.info(f"Preprocessed data for {data_id}")
    return data