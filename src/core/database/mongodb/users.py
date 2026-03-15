import logging
from typing import Dict, Any, Optional
from bson import ObjectId
from .connection import get_mongo_collection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)

def create_user_in_mongodb(username: str, email: str = None, phone_number: str = None) -> Optional[str]:
    """Creates a new user document in the MongoDB users collection."""
    collection = get_mongo_collection("users")
    if collection is None:
        logger.error("Failed to access MongoDB users collection.")
        return None

    try:
        user_doc = {
            "username": username,
            "email": email,
            "phone_number": phone_number,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "active_replies_workflow": None,
            "active_messages_workflow": None,
            "active_retweets_workflow": None
        }
        result = collection.insert_one(user_doc)
        logger.info(f"✅ Created user in MongoDB with ID: {result.inserted_id}")
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"❌ Error creating user in MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error creating user in MongoDB: {str(e)}")
        return None

def update_user_in_mongodb(user_id: str, updates: Dict[str, Any]):
    """Updates a user document in the MongoDB users collection."""
    collection = get_mongo_collection("users")
    if collection is None:
        logger.error("Failed to access MongoDB users collection.")
        return

    try:
        updates["updated_at"] = datetime.now()
        result = collection.update_one({"_id": ObjectId(user_id)}, {"$set": updates})
        if result.modified_count > 0:
            logger.info(f"✅ Updated user {user_id} in MongoDB")
        else:
            logger.warning(f"No user found with ID {user_id} or no changes applied")
    except Exception as e:
        logger.error(f"❌ Error updating user {user_id} in MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error updating user in MongoDB: {str(e)}")