import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId
from .connection import get_mongo_collection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

def log_link_update_in_mongodb(postgres_link_id: int, user_id: int, workflow_id: str) -> Optional[str]:
    """Logs a link update in the MongoDB links_updated collection."""
    collection = get_mongo_collection("links_updated")
    if collection is None:
        logger.error("Failed to access MongoDB links_updated collection.")
        if STREAMLIT_AVAILABLE:
            st.error("❌ Failed to access MongoDB links_updated collection.")
        return None

    try:
        update_doc = {
            "postgres_links_id": postgres_link_id,
            "user_id": user_id,
            "workflow_id": workflow_id,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        result = collection.insert_one(update_doc)
        logger.info(f"✅ Logged link update in MongoDB with ID: {result.inserted_id}")
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"❌ Error logging link update in MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error logging link update in MongoDB: {str(e)}")
        return None

def get_links_updated_from_mongodb(limit: int = None) -> List[Dict[str, Any]]:
    """Fetches link update records from the MongoDB links_updated collection."""
    collection = get_mongo_collection("links_updated")
    if collection is None:
        logger.error("Failed to access MongoDB links_updated collection.")
        if STREAMLIT_AVAILABLE:
            st.error("❌ Failed to access MongoDB links_updated collection.")
        return []

    try:
        cursor = collection.find()
        if limit is not None:
            cursor = cursor.limit(limit)
        updates = list(cursor)
        for update in updates:
            update['_id'] = str(update['_id'])
        logger.info(f"Retrieved {len(updates)} link update records from MongoDB.")
        return updates
    except Exception as e:
        logger.error(f"Error fetching link updates from MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error fetching link updates from MongoDB: {str(e)}")
        return []