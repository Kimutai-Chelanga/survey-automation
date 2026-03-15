from pymongo import MongoClient
from .config import MONGODB_URI, logger
from core.database.postgres.connection import get_postgres_connection

def get_mongo_connection():
    """Get MongoDB connection"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client['messages_db']
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise