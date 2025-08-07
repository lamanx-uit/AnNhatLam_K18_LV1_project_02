import pandas as pd
import requests as req
import time
from requests.adapters import HTTPAdapter

def read_data():
    # Read the Excel file
    df = pd.read_excel('/home/lamanx/DEC/Lab2/data/input/products-0-200000.xlsx')
    product_ids = df['id'].tolist()  # list product ID
    product_ids = dict.fromkeys(product_ids)  # remove duplicates
    product_ids = list(product_ids.keys())  # convert back to list
    return product_ids

def setup_session():
    session = req.Session()
    session.mount('https://', HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100
    ))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Accept': 'application/json, */*',
        'Accept-Language': 'en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7,vi;q=0.6',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Sec-Ch-Ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Referer': 'https://tiki.vn/',
        'Origin': 'https://tiki.vn'
    }
    return session, headers

def get_product_data(product_id):
    max_retries = 5
    session, headers = setup_session()
    url = f'https://api.tiki.vn/product-detail/api/v1/products/{product_id}'
    print(f"Fetching data for product ID {product_id}")
    
    for attempt in range(max_retries):
        response = session.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            data = response.json()
            selected_data = {
                'id': data['id'],
                'name': data['name'], 
                'url_key': data['url_key'],
                'price': data['price'],
                'description': data['description'],
                'images_url': data['images']  
            }
            print(f"Fetched data for product ID {product_id}")
            return selected_data
        
        elif response.status_code in [429, 500]:
            print(f"Rate limit exceeded for product ID {product_id}, retrying...")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt) 
                
        else:
            print(f"Error fetching data for product ID {product_id}: {response.status_code}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  
            return None
        
    print(f"Failed to fetch data for product ID {product_id} after {max_retries} attempts")
    return None

"""Vấn đề:
- The program can't ensure that the data outcome is the same all the times -> Veracity is really really bad
- The Velocity is equal to average, which should be improve if using asyncio. But the speed isn't too terrible. So this one PASSED
- Fault tolerence: Some, but is handling incorrectly. Therefore the re-try mechanism seems to be useless.
- Settings enviroment is trash, still have to use relative path, which is definitely not good
- The logs are not detail enough, nor accessable enough. Should add more logs and write into a seperate file in the logs folder.
- 8/8, you're gonna fix all of these promblem, then, we come to the advance part: health check and self-healing
- If time allows, crawl from 3 websites: Lazada, Shopee and Tiki. (After ensure all of the problems are fixed ofc)
- This code is overwrite new batches if the file already exists, which is not good. Should add preserve and enable versioning to the filename."""