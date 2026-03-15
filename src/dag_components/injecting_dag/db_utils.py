import os
import logging
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse
from contextlib import contextmanager
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import MONGO_URI, POSTGRES_URI

logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_mongo_db():
    """Get MongoDB database connection with retry logic"""
    try:
        mongo_client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=30000,  # 30 seconds
            connectTimeoutMS=20000,  # 20 seconds
            socketTimeoutMS=20000,   # 20 seconds
            maxPoolSize=10,
            retryWrites=True
        )
        
        # Test the connection
        mongo_client.admin.command('ping')
        db = mongo_client.get_database()
        
        logger.debug("✅ MongoDB connection established")
        return db, mongo_client
        
    except Exception as e:
        logger.error(f"❌ MongoDB connection error: {e}")
        raise Exception(f"Failed to connect to MongoDB: {e}")

@contextmanager
def get_postgres_connection():
    """Get PostgreSQL connection with proper error handling and context management"""
    conn = None
    try:
        # Parse the DATABASE_URL to extract connection parameters
        parsed_url = urlparse(POSTGRES_URI)
        
        # Connect using parsed parameters with enhanced configuration
        conn = psycopg2.connect(
            host=parsed_url.hostname,
            port=parsed_url.port or 5432,
            dbname=parsed_url.path.lstrip("/"),
            user=parsed_url.username,
            password=parsed_url.password,
            cursor_factory=RealDictCursor,
            connect_timeout=30,
            application_name='airflow_workflow_injection'
        )
        
        # Set connection to autocommit mode for better performance
        conn.autocommit = True
        
        logger.info("✅ PostgreSQL connection established")
        yield conn
        
    except psycopg2.OperationalError as e:
        logger.error(f"❌ PostgreSQL operational error: {e}")
        raise Exception(f"Failed to connect to PostgreSQL (operational): {e}")
    except psycopg2.Error as e:
        logger.error(f"❌ PostgreSQL connection error: {e}")
        raise Exception(f"Failed to connect to PostgreSQL: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error connecting to PostgreSQL: {e}")
        raise Exception(f"Failed to connect to PostgreSQL: {e}")
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("✅ PostgreSQL connection closed")
            except Exception as e:
                logger.error(f"❌ Error closing PostgreSQL connection: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def get_postgres_connection_legacy():
    """Legacy function for backward compatibility - returns connection directly"""
    try:
        parsed_url = urlparse(POSTGRES_URI)
        
        conn = psycopg2.connect(
            host=parsed_url.hostname,
            port=parsed_url.port or 5432,
            dbname=parsed_url.path.lstrip("/"),
            user=parsed_url.username,
            password=parsed_url.password,
            cursor_factory=RealDictCursor,
            connect_timeout=30,
            application_name='airflow_workflow_injection_legacy'
        )
        
        conn.autocommit = True
        logger.info("✅ PostgreSQL connection established (legacy)")
        return conn
        
    except psycopg2.OperationalError as e:
        logger.error(f"❌ PostgreSQL operational error (legacy): {e}")
        raise Exception(f"Failed to connect to PostgreSQL (operational): {e}")
    except psycopg2.Error as e:
        logger.error(f"❌ PostgreSQL connection error (legacy): {e}")
        raise Exception(f"Failed to connect to PostgreSQL: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error connecting to PostgreSQL (legacy): {e}")
        raise Exception(f"Failed to connect to PostgreSQL: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def get_system_setting(setting_key, default_value):
    """Get system setting from MongoDB settings collection with retry logic"""
    db = None
    client = None
    try:
        db, client = get_mongo_db()
        settings_collection = db['settings']
        
        # Try to find the settings document
        settings_doc = settings_collection.find_one({'category': 'system'})
        
        if settings_doc and 'settings' in settings_doc:
            result = settings_doc['settings'].get(setting_key, default_value)
            logger.debug(f"✅ Retrieved setting {setting_key}: {result}")
        else:
            logger.warning(f"⚠️ No system settings found, using default for {setting_key}: {default_value}")
            result = default_value
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error fetching setting {setting_key}: {e}")
        return default_value
    finally:
        if client:
            try:
                client.close()
            except Exception as e:
                logger.error(f"❌ Error closing MongoDB connection: {e}")

def test_database_connections():
    """Test both MongoDB and PostgreSQL connections"""
    results = {
        'mongodb': {'status': 'unknown', 'error': None},
        'postgresql': {'status': 'unknown', 'error': None}
    }
    
    # Test MongoDB
    try:
        db, client = get_mongo_db()
        collections = db.list_collection_names()
        results['mongodb']['status'] = 'connected'
        results['mongodb']['collections_count'] = len(collections)
        client.close()
        logger.info(f"✅ MongoDB test successful: {len(collections)} collections found")
    except Exception as e:
        results['mongodb']['status'] = 'error'
        results['mongodb']['error'] = str(e)
        logger.error(f"❌ MongoDB test failed: {e}")
    
    # Test PostgreSQL
    try:
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            results['postgresql']['status'] = 'connected'
            results['postgresql']['version'] = version[0] if version else 'Unknown'
            cursor.close()
            logger.info(f"✅ PostgreSQL test successful: {version[0] if version else 'Unknown version'}")
    except Exception as e:
        results['postgresql']['status'] = 'error'
        results['postgresql']['error'] = str(e)
        logger.error(f"❌ PostgreSQL test failed: {e}")
    
    return results

def get_database_health_stats():
    """Get health statistics for both databases"""
    stats = {
        'mongodb': {
            'status': 'unknown',
            'collections': {},
            'total_documents': 0
        },
        'postgresql': {
            'status': 'unknown',
            'tables': {},
            'total_rows': 0
        }
    }
    
    # MongoDB health stats
    try:
        db, client = get_mongo_db()
        
        workflow_collections = ['messages_workflows', 'replies_workflows', 'retweets_workflows']
        total_docs = 0
        
        for collection_name in workflow_collections:
            if collection_name in db.list_collection_names():
                collection = db[collection_name]
                doc_count = collection.count_documents({})
                executed_count = collection.count_documents({'executed': True})
                
                stats['mongodb']['collections'][collection_name] = {
                    'total_documents': doc_count,
                    'executed_documents': executed_count,
                    'pending_documents': doc_count - executed_count
                }
                total_docs += doc_count
        
        stats['mongodb']['total_documents'] = total_docs
        stats['mongodb']['status'] = 'healthy'
        client.close()
        
    except Exception as e:
        stats['mongodb']['status'] = 'error'
        stats['mongodb']['error'] = str(e)
        logger.error(f"❌ MongoDB health check failed: {e}")
    
    # PostgreSQL health stats
    try:
        with get_postgres_connection() as conn:
            cursor = conn.cursor()
            
            # Get links table stats
            cursor.execute("SELECT COUNT(*) FROM links;")
            links_count = cursor.fetchone()[0] if cursor.rowcount > 0 else 0
            
            stats['postgresql']['tables']['links'] = {
                'total_rows': links_count
            }
            stats['postgresql']['total_rows'] = links_count
            stats['postgresql']['status'] = 'healthy'
            cursor.close()
            
    except Exception as e:
        stats['postgresql']['status'] = 'error'
        stats['postgresql']['error'] = str(e)
        logger.error(f"❌ PostgreSQL health check failed: {e}")
