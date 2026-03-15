import os
import time
import logging
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse
from contextlib import contextmanager

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

# Connection pool
_postgres_pool = None

# --- Configure Logging ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# --- Database URL ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/messages")

def init_connection_pool():
    """Initialize the connection pool."""
    global _postgres_pool
    if _postgres_pool:
        try:
            conn = _postgres_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            _postgres_pool.putconn(conn)
            logger.info("✅ PostgreSQL connection pool is active")
            return _postgres_pool
        except psycopg2.Error as e:
            logger.warning(f"PostgreSQL pool connection test failed: {e}")
            _postgres_pool.closeall()
            _postgres_pool = None

    try:
        parsed_url = urlparse(DATABASE_URL)
        _postgres_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=8,
            host=parsed_url.hostname,
            port=parsed_url.port or 5432,
            dbname=parsed_url.path.lstrip("/"),
            user=parsed_url.username,
            password=parsed_url.password,
            cursor_factory=RealDictCursor
        )
        logger.info("✅ PostgreSQL connection pool initialized")
        return _postgres_pool
    except Exception as e:
        logger.error(f"❌ Error creating PostgreSQL connection pool: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"❌ Error creating PostgreSQL connection pool: {str(e)}")
        return None

@contextmanager
def get_postgres_connection():
    """Get a connection from the pool with proper cleanup and transaction management."""
    conn = None
    try:
        if not _postgres_pool:
            init_connection_pool()

        if not _postgres_pool:
            raise Exception("Connection pool not available")

        conn = _postgres_pool.getconn()

        # IMPORTANT: Reset connection state to avoid "already in transaction" errors
        # This ensures we start with a clean slate
        conn.rollback()  # Clear any existing transaction

        # Test connection
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")

        logger.info("Retrieved PostgreSQL connection from pool")
        yield conn

        # Auto-commit if no exception occurred
        if conn and not conn.closed:
            conn.commit()

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error: {e}")
        if conn and not conn.closed:
            conn.rollback()  # Rollback on error
        if STREAMLIT_AVAILABLE:
            st.error(f"Database error: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Connection error: {e}")
        if conn and not conn.closed:
            conn.rollback()  # Rollback on error
        raise e
    finally:
        if conn and _postgres_pool:
            try:
                # Ensure connection is in a clean state before returning to pool
                if not conn.closed:
                    conn.rollback()  # Clear any pending transaction
                _postgres_pool.putconn(conn)
                logger.debug("Connection returned to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")

def close_all_connections():
    """Close all connections in the pool."""
    global _postgres_pool
    if _postgres_pool:
        _postgres_pool.closeall()
        logger.info("✅ PostgreSQL connection pool closed")
        _postgres_pool = None
