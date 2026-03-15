import os
import logging
from pymongo import MongoClient, errors
from pymongo.server_api import ServerApi
from typing import Optional

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

# Global MongoDB client
_client: Optional[MongoClient] = None

# --- Configure Logging ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# --- MongoDB Configuration ---
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "messages_db")


def get_mongo_client() -> Optional[MongoClient]:
    """Creates and returns a MongoDB client with connection pooling. Returns None on failure."""
    global _client

    # Reuse existing healthy client
    if _client is not None:
        try:
            _client.admin.command('ping')
            logger.debug("MongoDB client ping successful (reused)")
            return _client
        except errors.PyMongoError as e:
            logger.warning(f"MongoDB connection lost. Reconnecting... Error: {e}")
            _client = None  # Force reconnect

    # Create new client
    try:
        _client = MongoClient(
            MONGO_URI,
            server_api=ServerApi('1'),
            maxPoolSize=10,
            minPoolSize=3,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
            serverSelectionTimeoutMS=5000
        )
        # Test connection
        _client.admin.command('ping')
        logger.info(f"MongoDB connected successfully to {MONGO_URI.split('@')[-1].split('/')[0]}")
        return _client

    except errors.PyMongoError as e:
        logger.error(f"MongoDB connection failed: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"MongoDB connection failed: {str(e)}")
        _client = None
        return None


def get_mongo_collection(collection_name: str):
    """
    Returns a MongoDB collection.
    Always returns None on failure — safe for `is None` checks.
    """
    if not collection_name or not isinstance(collection_name, str):
        logger.error("Invalid collection name provided")
        return None

    client = get_mongo_client()
    if client is None:
        logger.error(f"Cannot access collection '{collection_name}': No MongoDB client")
        return None

    try:
        db = client[MONGO_DB]
        collection = db[collection_name]

        # Optional: Light validation that collection exists
        if collection_name not in db.list_collection_names():
            logger.warning(f"Collection '{collection_name}' does not exist in database '{MONGO_DB}'")
            # Still return it — PyMongo allows operations on non-existent collections
            # But warn early

        logger.debug(f"Accessed collection: {MONGO_DB}.{collection_name}")
        return collection

    except Exception as e:
        logger.error(f"Failed to get collection '{collection_name}': {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Database error: {str(e)}")
        return None