# %%
import pandas as pd
import requests as req
import json
import time
import openpyxl as px
import html
import re
from bs4 import BeautifulSoup
import threading
import queue

# %%
df = pd.read_excel('products-0-200000.xlsx')

# %%
product_id = df['id'].tolist()  # list product ID

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,fr-FR;q=0.8,fr;q=0.7,vi;q=0.6',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }
    
"""DoD: Sử dụng code Python, tải về thông tin của 200k sản phẩm (list product id bên dưới) của Tiki và lưu thành các file .json. 
Mỗi file có thông tin của khoảng 1000 sản phẩm. 
Các thông in cần lấy bao gồm: id, name, url_key, price, description, images url. 
Yêu cầu chuẩn hoá nội dung trong "description" và tìm phương án rút ngắn thời gian lấy dữ liệu.
- List product_id: https://1drv.ms/u/s!AukvlU4z92FZgp4xIlzQ4giHVa5Lpw?e=qDXctn
- API get product detail: https://api.tiki.vn/product-detail/api/v1/products/138083218"""

def get_product_data(product_id, q):
    
    url = f'https://api.tiki.vn/product-detail/api/v1/products/{product_id}'
    session = req.Session()
    print(f"Fetching data for product ID {product_id}")
    response = session.get(url, headers=headers)
    
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
        q.put(selected_data)
        print(f"Got data for {product_id} \n")
        return selected_data
    else:
        data = response.json()
        error_data = {
            'id': product_id,
            'description': f'Error {response.status_code}', 
            'errors': data['errors']
        }
        q.put(error_data)
        print(f"Error fetching data for product ID {product_id}: {response.status_code}")
        return None 

# %%
def preprocessing(q_in, q_out):
    data = q_in.get()
    text = data['description']
    text = html.unescape(text)
    text = BeautifulSoup(text, 'html.parser').get_text()
    text = re.sub(r'\s+', ' ', text).strip()
    data['description'] = text
    q_out.put(data)
    print(f"Preprocessed data for {data['id']}")

# %%
data_queue=queue.Queue()
result_queue=queue.Queue()

# %%
# Multi threading test
thread = []
for i in product_id[0:1000]:
    if len(thread) > 100:
        for t in thread:
            t.join()
        thread = []
    t1 = threading.Thread(target=get_product_data, args=(i, data_queue))
    t1.start()
    thread.append(t1)
    
    t2 = threading.Thread(target=preprocessing, args=(data_queue, result_queue))
    t2.start()
    thread.append(t2)

# %%
def batching():
    for i in range(0, len(product_id), 1000):
        batch = product_id[i:i+1000]
        print(f"Processing batch {i//1000 + 1}")

        for single_product in batch:
            t1 = threading.Thread(target=get_product_data, args=(single_product, data_queue))
            t1.start()
            t1.join()
        
        time.sleep(1)
        
        t2 = threading.Thread(target=preprocessing, args=(data_queue, result_queue))
        t2.start()
        t2.join()

# %%
batching()


