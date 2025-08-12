import json
import pandas as pd
import requests as req
import time
from requests.adapters import HTTPAdapter
import random
import logging
from pathlib import Path
import requests
from circuitbreaker import circuit

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
    'output': BASE_DIR / 'tests' / 'output'
}

def setup_logging():
    logging.basicConfig(
        filename=FILES['logs'],
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Logging setup complete.")

def read_data():
    """Read product IDs, check and improve veracity."""
    # Read the Excel file
    df = pd.read_excel(FILES['products'])
    logging.info(f"Read input file successfully")
    
    product_ids = df['id'].tolist()  # list product IDs
    logging.info(f"Extracted {len(product_ids)} product IDs from the input file")

    product_ids = dict.fromkeys(product_ids)  # remove duplicates
    logging.info(f"Removed duplicates successfully, {len(product_ids)} unique product IDs remaining")
    product_ids = list(product_ids.keys())  
    
    # Ensure product IDs are in appropriate format
    valid_ids = []
    invalid_count = 0
    for pid in product_ids:
       if isinstance(pid, (int, float)) and str(int(pid)).isdigit() and pid > 0 and pid == int(pid):
           valid_ids.append(int(pid))
       else:
           invalid_count += 1
           logging.warning(f"Invalid product ID found: {pid} (type: {type(pid)})")
           logging.info(f'{invalid_count} invalid IDs removed')
    logging.info(f"Validation complete: {len(valid_ids)} valid IDs")
    # There are thousands of product IDs that can be NOT right
    # Therefore, I decided to give it a DLQ so that it can be improved further in the future.
    # Right now the data is clean enough so we can proceed with the valid IDs
    valid_ids_set = set(valid_ids) 
    invalid_id = [pid for pid in product_ids if pid not in valid_ids_set]
    if len(invalid_id) > 0:
        with open (FILES['abnormal-id'], 'w') as f:
            f.write('\n'.join(map(str, invalid_id)) + '\n')
        logging.info(f"Invalid IDs written to {FILES['abnormal-id']}") 
    product_ids = valid_ids
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
    max_retries = 3
    session, headers = setup_session()
    url = f'https://api.tiki.vn/product-detail/api/v1/products/{product_id}'
    logging.info(f"Fetching data for product ID {product_id}")
    last_error = str()

    for attempt in range(max_retries):
        try: 
            response = session.get(url, headers=headers, timeout=15)
            # If successful, parse the JSON response
            if response.status_code == 200:
                try: 
                    data = response.json()
                except ValueError as e:
                    logging.error(f"Error parsing JSON for product ID {product_id}: {e}")
                    continue
                
                selected_data = {
                    'id': data.get('id', None),
                    'name': data.get('name', None),
                    'url_key': data.get('url_key', None),
                    'price': data.get('price', None), 
                    'description': data.get('description', None),
                    'images_url': data.get('images', None)
                }
                logging.info(f"Fetched data for product ID {product_id}")
                return selected_data
            elif response.status_code == 404:
                logging.warning(f"Product ID {product_id} not found")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3) * (attempt + 1))
                else: 
                    last_error = f"{response.status_code} after {max_retries} attempts"

            elif response.status_code in [429, 500, 502, 503, 504]:
                logging.warning(f"Rate limit exceeded or Internal server error for product ID {product_id}, retrying...")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3) * (attempt + 1))  
                else: 
                    last_error = f"{response.status_code} after {max_retries} attempts"

        except req.Timeout:
            logging.warning(f"Timeout error for product ID {product_id}, retrying...")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3) * (attempt + 1))
            else:
                last_error = f"Timeout after {max_retries} attempts"
            raise req.Timeout()
            
        except req.ConnectionError:
            logging.warning(f"Connection error for product ID {product_id}, retrying...")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3) * (attempt + 1))
            else:
                last_error = f"Connection error after {max_retries} attempts"
            raise req.ConnectionError()
        
        # Other errors
        except req.RequestException as e:
            error_reason = str(e)
            logging.error(f"Request error for product ID {product_id}: {error_reason}, retrying...")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(1, 3) * (attempt + 1))
            else:
                last_error = f"{error_reason} error after {max_retries} attempts"

    # If all retries fail, to DLQ
    logging.error(f"Failed to fetch data for product ID {product_id} after {max_retries} attempts, {last_error}")
    # Log the error to DLQ logs
    with open(FILES['abnormal-id'], 'a') as f:
        f.write(f"{product_id}, {last_error}\n")     
    return None

"""TODO:
- The program can't ensure that the data outcome is the same all the times -> Veracity is really really bad (PASSED)
- The Velocity is equal to average, which should be improve if using asyncio. But the speed isn't too terrible. So this one (PASSED)
- The variety sucks though, the data is not diverse enough, come from only one source, which is a problem. (LATER)
- Fault tolerence: Some, but is handling incorrectly. Therefore the re-try mechanism seems to be useless, 
should be combined with health-check and self healing, those cant be processed -> to DLQ. (HALFWAY)

- Settings enviroment is trash, still have to use relative path, which is definitely not good (PASSED)

- The logs are not detail enough, nor accessable enough. Should add more logs and write into a seperate file in the logs folder, add DLQ,
if you have so much time, send em to S3, after that provoke a lambda function to send logs through SNS lol. (LOCAL PASSED, CLOUD LATER)

- 8/8, you're gonna fix all of these promblem, then, we come to the advanced part: health check and self-healing
ok it's time: implement circuilt + fallback, add healthcheck and i think thats it (DOING)
- If time allows, crawl from 3 websites: Lazada, Shopee and Tiki. (After ensure all of the problems are fixed ofc)
- This code is overwrite new batches if the file already exists, which is not good. Should add preserve and enable versioning to the filename. """