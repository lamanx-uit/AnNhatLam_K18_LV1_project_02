import json
import logging
from pathlib import Path
import psycopg2
from configparser import ConfigParser

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

    'checkpoint' : BASE_DIR / 'config' / 'checkpoints' / 'checkpoint.json',
    'archive' : BASE_DIR / 'config' / 'checkpoints' / 'archive'
}

def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def get_data(datafile):
    with open(datafile, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def import_products(data, conn):
    sql = """
        INSERT INTO products (product_id, name, price, url_key, description)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING
    """
    result = {
        'summary': { 
            'total': len(data),
            'successful': 0,
            'failed': 0,
            'skipped': 0
        },
        'inserted_products': [],
        'failed_products': [],
    }
    try:
        with conn.cursor() as cur:
            for product in data:
                try:
                    product_data = (
                        product.get('id', ""),
                        product.get('name', ""),
                        product.get('price', ""),
                        product.get('url_key', ""),
                        product.get('description', ""),
                    )

                    cur.execute(sql, product_data)

                    if cur.rowcount > 0:  # Check if actually inserted
                        # New insert - but no RETURNING needed với ON CONFLICT
                        product_id = product.get('id')  # Use original ID
                        result['inserted_products'].append({'id': product_id})
                        result['summary']['successful'] += 1
                    else:
                        # Skipped due to conflict - not an error
                        print(f"Product {product.get('id')} already exists, skipped") 
                        result['summary']['skipped'] += 1               
                    
                except (Exception, psycopg2.DatabaseError) as error:
                    print(f"Error inserting product: {error}")
                    result['failed_products'].append({'id': product.get('id', ""), 'error': str(error)})
                    result['summary']['failed'] += 1
                    continue

            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting product: {error}")
    finally:
        return result

def import_images(data, conn):
    sql = """
        INSERT INTO images (product_id, images_url)
        VALUES (%s, %s)
        ON CONFLICT (product_id, images_url) DO NOTHING
    """
    result = {
        'summary': { 
            'total': len(data),
            'successful': 0,
            'failed': 0,
            'skipped': 0
        },
        'inserted_products': [],
        'failed_products': [],
    }
    
    try:
        with conn.cursor() as cur:
            for product in data:
                try:
                    # Define right structure for image_url
                    id = product.get('id', "")
                    urls = product.get('images_url', [])
                    d = [(id, url) for url in urls]

                    cur.executemany(sql, d)
                    row_updated = cur.rowcount  
                    
                     # Check if actually inserted
                    if row_updated > 0: 
                        # New insert - but no RETURNING needed with on conflict
                        product_id = product.get('id')
                        result['inserted_products'].append({'id': product_id})
                        result['summary']['successful'] += 1
                    else:
                        # Skipped due to conflict
                        print(f"Product {product.get('id')} already exists, skipped") 
                        result['summary']['skipped'] += 1               
                        
                    print(f"Inserted {row_updated} images for product {id}")

                except (Exception, psycopg2.DatabaseError) as error:
                    print(f"Error inserting product: {error}")
                    result['failed_products'].append({'id': product.get('id', ""), 'error': str(error)})
                    result['summary']['failed'] += 1
                    continue

            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting product: {error}")  
    finally:
        return result

def process_db_main(data_file):
    conn = connect(load_config())

    retry_attempts = 3

    for i in range(1, 3):
        data_file = f'/batch_{i}.json'
            
        data = get_data(data_file)

        for attempt in range(retry_attempts):
            try:
                import_products(data, conn)
                import_images(data, conn)
                break  # Exit retry loop if successful
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == retry_attempts - 1:
                    print("Max retries reached. Skipping this batch.")
    return None
