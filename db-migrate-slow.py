import logging
import time
from bson import ObjectId
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = 'mongodb://root:root@localhost:27019'
DB_NAME = 'products'
COLLECTION_NAME = 'gmc_products'
TARGET_COLLECTION_NAME = 'new_collection'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
target_collection = db[TARGET_COLLECTION_NAME]

def fetch_products(batch_size, last_id=None):
    query = {'_id': {'$gt': last_id}} if last_id else {}
    cursor = collection.find(query).sort('_id').limit(batch_size)
    products = list(cursor)
    return products

def bulk_write_to_mongo(products):
    for product in products:
        product['_id'] = ObjectId()

    try:
        result = target_collection.insert_many(products, ordered=False)
        logger.info(f'Inserted {len(result.inserted_ids)} products into MongoDB.')
    except Exception as e:
        logger.error(f'Error inserting products into MongoDB: {e}')

def process_batches(batch_size):
    last_id = None
    while True:
        products = fetch_products(batch_size, last_id)
        if not products:
            break
        last_id = products[-1]['_id']
        bulk_write_to_mongo(products)

def main():
    batch_size = 1000

    start_time = time.time()
    process_batches(batch_size)
    end_time = time.time()
    logger.info(f'Total time: {end_time - start_time:.2f} seconds.')

if __name__ == '__main__':
    main()
