"""
MongoDB connection helper for account media dashboard.
Place this in: src/core/database/mongodb.py
"""

import os
import logging
from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from contextlib import contextmanager

# Configure logging
logger = logging.getLogger(__name__)

# MongoDB configuration from environment variables
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@localhost:27017/messages_db?authSource=admin')
MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'messages_db')

# Global MongoDB client (singleton pattern)
_mongo_client: Optional[MongoClient] = None


def get_mongodb_client() -> MongoClient:
    """Get or create MongoDB client singleton."""
    global _mongo_client
    
    if _mongo_client is None:
        try:
            _mongo_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000
            )
            # Test connection
            _mongo_client.admin.command('ping')
            logger.info("MongoDB connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    return _mongo_client


def get_mongodb_connection() -> Database:
    """Get MongoDB database connection."""
    client = get_mongodb_client()
    return client[MONGODB_DB_NAME]


@contextmanager
def get_mongodb_session():
    """Context manager for MongoDB operations with automatic session handling."""
    client = get_mongodb_client()
    db = client[MONGODB_DB_NAME]
    
    try:
        yield db
    except Exception as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise
    finally:
        # Connection pooling handles cleanup automatically
        pass


def close_mongodb_connection():
    """Close MongoDB connection (call on application shutdown)."""
    global _mongo_client
    
    if _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
        logger.info("MongoDB connection closed")


# Helper functions for common queries

def get_account_media_summary(postgres_account_id: int) -> dict:
    """Get media summary for an account."""
    try:
        db = get_mongodb_connection()
        
        # Get execution sessions summary
        sessions_pipeline = [
            {'$match': {'postgres_account_id': postgres_account_id}},
            {'$group': {
                '_id': None,
                'total_sessions': {'$sum': 1},
                'total_workflows': {'$sum': '$workflows_executed'},
                'successful_workflows': {'$sum': '$successful_workflows'},
                'total_screenshots': {'$sum': {'$size': {'$ifNull': ['$screenshots', []]}}},
                'sessions_with_video': {
                    '$sum': {'$cond': [{'$ne': ['$video_recording_url', None]}, 1, 0]}
                }
            }}
        ]
        
        sessions_result = list(db.execution_sessions.aggregate(sessions_pipeline))
        
        # Get video recordings summary
        videos_count = db.video_recordings.count_documents({
            'postgres_account_id': postgres_account_id
        })
        
        # Get screenshots summary
        screenshots_pipeline = [
            {'$match': {'postgres_account_id': postgres_account_id}},
            {'$group': {
                '_id': '$category',
                'count': {'$sum': 1}
            }}
        ]
        
        screenshots_by_category = {
            item['_id']: item['count']
            for item in db.screenshot_metadata.aggregate(screenshots_pipeline)
        }
        
        summary = {
            'sessions': sessions_result[0] if sessions_result else {},
            'total_video_recordings': videos_count,
            'screenshots_by_category': screenshots_by_category
        }
        
        return summary
    
    except Exception as e:
        logger.error(f"Error getting account media summary: {e}")
        return {}


def get_recent_sessions(postgres_account_id: int, limit: int = 10) -> list:
    """Get recent execution sessions for an account."""
    try:
        db = get_mongodb_connection()
        
        sessions = db.execution_sessions.find(
            {'postgres_account_id': postgres_account_id}
        ).sort('created_at', -1).limit(limit)
        
        return list(sessions)
    
    except Exception as e:
        logger.error(f"Error getting recent sessions: {e}")
        return []


def get_screenshots_by_session(session_id: str) -> list:
    """Get all screenshots for a specific session."""
    try:
        db = get_mongodb_connection()
        
        screenshots = db.screenshot_metadata.find(
            {'session_id': session_id}
        ).sort('created_at', -1)
        
        return list(screenshots)
    
    except Exception as e:
        logger.error(f"Error getting screenshots for session: {e}")
        return []


def get_video_recording(session_id: str) -> Optional[dict]:
    """Get video recording for a specific session."""
    try:
        db = get_mongodb_connection()
        
        video = db.video_recordings.find_one({'session_id': session_id})
        return video
    
    except Exception as e:
        logger.error(f"Error getting video recording: {e}")
        return None


def get_profile_media_stats(profile_id: str) -> dict:
    """Get media statistics for a specific profile."""
    try:
        db = get_mongodb_connection()
        
        # Check if profile_media_summary view exists
        view_stats = db.profile_media_summary.find_one({'profile_id': profile_id})
        
        if view_stats:
            return dict(view_stats)
        
        # Fallback to manual aggregation
        pipeline = [
            {'$match': {'profile_id': profile_id}},
            {'$group': {
                '_id': None,
                'total_sessions': {'$sum': 1},
                'total_screenshots': {'$sum': '$screenshot_count'},
                'total_workflows': {'$sum': '$workflows_executed'},
                'completed_videos': {
                    '$sum': {'$cond': [
                        {'$eq': ['$video_recording_status', 'completed']}, 
                        1, 
                        0
                    ]}
                }
            }}
        ]
        
        result = list(db.video_recordings.aggregate(pipeline))
        return result[0] if result else {}
    
    except Exception as e:
        logger.error(f"Error getting profile media stats: {e}")
        return {}


def get_gridfs_screenshot(gridfs_file_id: str) -> Optional[bytes]:
    """Retrieve screenshot from GridFS."""
    try:
        from gridfs import GridFS
        
        db = get_mongodb_connection()
        fs = GridFS(db, collection='screenshots')
        
        if fs.exists(gridfs_file_id):
            grid_out = fs.get(gridfs_file_id)
            return grid_out.read()
        
        return None
    
    except Exception as e:
        logger.error(f"Error retrieving GridFS file: {e}")
        return None


def search_media_by_date_range(
    postgres_account_id: int,
    start_date,
    end_date,
    media_type: str = 'all'
) -> dict:
    """Search for media within a date range."""
    try:
        db = get_mongodb_connection()
        
        query = {
            'postgres_account_id': postgres_account_id,
            'created_at': {
                '$gte': start_date,
                '$lte': end_date
            }
        }
        
        results = {}
        
        if media_type in ['all', 'screenshots']:
            screenshots = list(db.screenshot_metadata.find(query).sort('created_at', -1))
            results['screenshots'] = screenshots
        
        if media_type in ['all', 'videos']:
            videos = list(db.video_recordings.find(query).sort('created_at', -1))
            results['videos'] = videos
        
        if media_type in ['all', 'sessions']:
            sessions = list(db.execution_sessions.find(query).sort('created_at', -1))
            results['sessions'] = sessions
        
        return results
    
    except Exception as e:
        logger.error(f"Error searching media by date range: {e}")
        return {}