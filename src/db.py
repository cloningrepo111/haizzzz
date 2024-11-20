import os

from config import settings

import redis
from motor.motor_asyncio import AsyncIOMotorClient
from elasticsearch import AsyncElasticsearch

# MongoDB
client = AsyncIOMotorClient(
    settings.MONGO_DATABASE_URL,
    serverSelectionTimeoutMS=5000
)

# Redis

# Elastic


async def check_connection():
    try:
        con = await client.server_info()
        print(f'Connected to Asysc MongoDB {con.get("version")} at {settings.MONGO_DATABASE_URL}')
    except Exception as e:
        print(f"Unable to connect to the Async MongoDB server. Error: {e}")


db = client[settings.MONGO_INITDB_DATABASE]

SearchStage = db['search_stages']
SearchPipeline = db['search_pipelines']
SearchResult = db['search_results']
