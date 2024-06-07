import logging
import random
import asyncio
import time
import uvloop
from faker import Faker
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

MONGO_URI = 'mongodb://root:root@localhost:27020'
DB_NAME = 'products'
COLLECTION_NAME = 'gmc_products'

client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def generate_fake_product():
    return {
        'offerId': fake.uuid4(),
        'title': fake.catch_phrase(),
        'description': fake.text(),
        'link': fake.url(),
        'imageLink': fake.image_url(),
        'contentLanguage': 'en',
        'targetCountry': 'US',
        'channel': 'online',
        'availability': 'in stock',
        'condition': 'new',
        'price': {
            'value': str(round(random.uniform(10.0, 1000.0), 2)),
            'currency': 'USD'
        },
        'slug': {
            "US": "product-slug",
            "SA": "product-slug"
        },
        'brand': fake.company(),
        'gtin': str(fake.random_int(min=100000000000, max=999999999999)),
        'mpn': fake.bothify(text='???-########'),
    }

async def bulk_write_to_mongo(products):
    try:
        result = await collection.insert_many(products, ordered=False)
        logger.info(f'Inserted {len(result.inserted_ids)} products into MongoDB.')
    except Exception as e:
        logger.error(f'Error inserting products into MongoDB: {e}')

async def generate_and_insert_batches(batch_size, total_batches, concurrency_limit):
    tasks = []
    for _ in range(total_batches):
        products = [generate_fake_product() for _ in range(batch_size)]
        tasks.append(bulk_write_to_mongo(products))
        if len(tasks) >= concurrency_limit:
            await asyncio.gather(*tasks)
            tasks = []
    if tasks:
        await asyncio.gather(*tasks)

async def main():
    total_products = 1800000
    batch_size = 1000
    total_batches = total_products // batch_size
    concurrency_limit = 10

    start_time = time.time()
    await generate_and_insert_batches(batch_size, total_batches, concurrency_limit)
    end_time = time.time()
    logger.info(f'Total time: {end_time - start_time:.2f} seconds.')

if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(main())
