# File: core/database_manager.py
import logging
from typing import Optional
import streamlit as st

from src.core.database.postgres import (
    connection as pg_conn,
    schema,
    replies as pg_replies,
    messages as pg_messages,
    retweets as pg_retweets,
    links as pg_links,
    users as pg_users,
    workflow_limits,
    workflow_runs,
    workflow_generation_log,
    workflow_sync_log
)
from src.core.database.mongodb import (
    connection as mongo_conn,
    replies_workflows,
    messages_workflows,
    retweets_workflows,
    users as mongo_users,
    replies_updated,
    messages_updated,
    retweets_updated,
    links_updated
)

class DatabaseManager:
    """Manages database connections and initialization."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.mongo_client = None
        self.pg_pool = None
        
    def initialize(self):
        """Initialize all database connections."""
        self.logger.info("Starting database initialization...")
        self._init_mongodb()
        self._init_postgresql()
        self._ensure_schema()
        
    def _init_mongodb(self):
        """Initialize MongoDB connection."""
        self.logger.info("Connecting to MongoDB...")
        try:
            self.mongo_client = mongo_conn.get_mongo_client()
            if not self.mongo_client:
                self.logger.error("MongoDB connection failed.")
                st.error("❌ Failed to connect to MongoDB. Check credentials and server status.")
        except Exception as e:
            self.logger.error(f"MongoDB initialization error: {e}")
            st.error(f"❌ MongoDB initialization failed: {str(e)}")
    
    def _init_postgresql(self):
        """Initialize PostgreSQL connection."""
        self.logger.info("Connecting to PostgreSQL...")
        try:
            self.pg_pool = pg_conn.init_connection_pool()
            if not self.pg_pool:
                self.logger.error("PostgreSQL connection pool initialization failed.")
                st.error("❌ Failed to initialize PostgreSQL connection pool.")
            else:
                self.logger.info("PostgreSQL connection pool initialized successfully.")
        except AttributeError as e:
            self.logger.error(f"❌ PostgreSQL initialization failed: {e}")
            st.error(f"❌ PostgreSQL initialization failed: {str(e)}")
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during PostgreSQL initialization: {e}")
            st.error(f"❌ Unexpected PostgreSQL error: {str(e)}")
    
    def _ensure_schema(self):
        """Ensure database schema exists."""
        self.logger.info("Ensuring database schema...")
        try:
            schema.ensure_database()
            self.logger.info("Database schema ensured successfully.")
        except Exception as e:
            self.logger.error(f"❌ Schema creation failed: {e}")
            st.error(f"❌ Schema creation failed: {str(e)}")

    def execute_query(self, query, params=None, fetch=False):
        """Execute a SQL query on the PostgreSQL database.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters for the query. Defaults to None.
            fetch (bool, optional): Whether to fetch results. Defaults to False.

        Returns:
            If fetch is True, returns the fetched results. Otherwise, returns None.
        """
        if not self.pg_pool:
            raise Exception("PostgreSQL connection pool is not initialized.")

        conn = None
        try:
            conn = self.pg_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    result = cur.fetchall()
                    conn.commit()  # Commit even when fetching, especially for RETURNING clauses
                    return result
                else:
                    conn.commit()
                    return None
        except Exception as e:
            if conn:
                conn.rollback()  # Rollback on error
            self.logger.error(f"Error executing query: {e}")
            raise e
        finally:
            if conn:
                self.pg_pool.putconn(conn)

    def execute_query_with_return(self, query, params=None):
        """Execute a SQL query that returns data (like INSERT...RETURNING).
        
        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters for the query.
            
        Returns:
            The fetched results from the query.
        """
        if not self.pg_pool:
            raise Exception("PostgreSQL connection pool is not initialized.")

        conn = None
        try:
            conn = self.pg_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall()
                conn.commit()
                return result
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Error executing query with return: {e}")
            raise e
        finally:
            if conn:
                self.pg_pool.putconn(conn)