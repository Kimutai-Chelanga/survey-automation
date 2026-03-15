"""
Enhanced PostgreSQL Export Script with MongoDB Media Linkage
Exports PostgreSQL data and includes MongoDB media statistics for each account
"""

import psycopg2
import pandas as pd
import os
from datetime import datetime
from pymongo import MongoClient
from typing import Dict, List, Any

# Database connection parameters
DB_PARAMS = {
    'dbname': 'messages',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': '5432'
}

# MongoDB connection
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@localhost:27017/messages_db?authSource=admin')
MONGODB_DB = 'messages_db'

# Output directory for CSV files
OUTPUT_DIR = './data'
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

# Updated tables list for new schema
CORE_TABLES = [
    'accounts',
    'prompts',
    'workflows',
    'replies',
    'messages',
    'retweets',
    'links',
    'workflow_sync_log',
    'workflow_generation_log',
    'workflow_runs'
]

VIEWS = [
    'account_workflow_summary',
    'workflow_processing_status'
]


def get_mongodb_connection():
    """Establish MongoDB connection."""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGODB_DB]
        # Test connection
        db.command('ping')
        print("✓ Connected to MongoDB")
        return db
    except Exception as e:
        print(f"⚠ MongoDB connection failed: {e}")
        return None


def get_account_media_stats(mongo_db, account_id: int) -> Dict[str, Any]:
    """Get media statistics for an account from MongoDB."""
    if not mongo_db:
        return {}
    
    try:
        # Get execution sessions stats
        sessions_pipeline = [
            {'$match': {'postgres_account_id': account_id}},
            {'$group': {
                '_id': None,
                'total_sessions': {'$sum': 1},
                'total_workflows': {'$sum': '$workflows_executed'},
                'successful_workflows': {'$sum': '$successful_workflows'},
                'failed_workflows': {'$sum': '$failed_workflows'},
                'total_screenshots': {'$sum': {'$size': {'$ifNull': ['$screenshots', []]}}},
                'sessions_with_video': {
                    '$sum': {'$cond': [{'$ne': ['$video_recording_url', None]}, 1, 0]}
                },
                'total_execution_time': {'$sum': '$total_execution_time_seconds'}
            }}
        ]
        
        sessions_result = list(mongo_db.execution_sessions.aggregate(sessions_pipeline))
        
        # Get video recordings count
        videos_pipeline = [
            {'$match': {'postgres_account_id': account_id}},
            {'$group': {
                '_id': None,
                'total_videos': {'$sum': 1},
                'completed_videos': {
                    '$sum': {'$cond': [{'$eq': ['$video_recording_status', 'completed']}, 1, 0]}
                },
                'web_recordings': {
                    '$sum': {'$cond': [{'$eq': ['$web_recording_status', 'completed']}, 1, 0]}
                }
            }}
        ]
        
        videos_result = list(mongo_db.video_recordings.aggregate(videos_pipeline))
        
        # Get screenshots by category
        screenshots_pipeline = [
            {'$match': {'postgres_account_id': account_id}},
            {'$group': {
                '_id': '$category',
                'count': {'$sum': 1},
                'total_size': {'$sum': '$size'}
            }}
        ]
        
        screenshots_by_cat = list(mongo_db.screenshot_metadata.aggregate(screenshots_pipeline))
        
        stats = {
            'total_sessions': 0,
            'total_workflows': 0,
            'successful_workflows': 0,
            'failed_workflows': 0,
            'total_screenshots': 0,
            'sessions_with_video': 0,
            'total_execution_time_hours': 0,
            'total_video_recordings': 0,
            'completed_videos': 0,
            'web_recordings': 0,
            'screenshots_preExecution': 0,
            'screenshots_postExecution': 0,
            'screenshots_errors': 0,
            'screenshots_debug': 0,
            'total_screenshot_size_mb': 0
        }
        
        if sessions_result:
            sr = sessions_result[0]
            stats['total_sessions'] = sr.get('total_sessions', 0)
            stats['total_workflows'] = sr.get('total_workflows', 0)
            stats['successful_workflows'] = sr.get('successful_workflows', 0)
            stats['failed_workflows'] = sr.get('failed_workflows', 0)
            stats['total_screenshots'] = sr.get('total_screenshots', 0)
            stats['sessions_with_video'] = sr.get('sessions_with_video', 0)
            stats['total_execution_time_hours'] = sr.get('total_execution_time', 0) / 3600
        
        if videos_result:
            vr = videos_result[0]
            stats['total_video_recordings'] = vr.get('total_videos', 0)
            stats['completed_videos'] = vr.get('completed_videos', 0)
            stats['web_recordings'] = vr.get('web_recordings', 0)
        
        total_screenshot_size = 0
        for cat in screenshots_by_cat:
            category = cat['_id']
            count = cat['count']
            size = cat.get('total_size', 0)
            
            if category == 'preExecution':
                stats['screenshots_preExecution'] = count
            elif category == 'postExecution':
                stats['screenshots_postExecution'] = count
            elif category == 'errors':
                stats['screenshots_errors'] = count
            elif category == 'debug':
                stats['screenshots_debug'] = count
            
            total_screenshot_size += size
        
        stats['total_screenshot_size_mb'] = total_screenshot_size / (1024 * 1024)
        
        return stats
    
    except Exception as e:
        print(f"⚠ Error getting media stats for account {account_id}: {e}")
        return {}


def export_accounts_with_media(conn, mongo_db):
    """Export accounts table with MongoDB media statistics."""
    try:
        # Fetch accounts from PostgreSQL
        query = """
            SELECT 
                account_id,
                username,
                profile_id,
                created_time,
                updated_time,
                mongo_object_id,
                active_replies_workflow,
                active_messages_workflow,
                active_retweets_workflow,
                last_workflow_sync,
                total_replies_processed,
                total_messages_processed,
                total_retweets_processed,
                total_links_processed
            FROM accounts
            ORDER BY account_id
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Add MongoDB media statistics
        if mongo_db:
            print("  Enriching with MongoDB media statistics...")
            media_stats_list = []
            
            for _, row in df.iterrows():
                account_id = row['account_id']
                stats = get_account_media_stats(mongo_db, account_id)
                media_stats_list.append(stats)
            
            # Add media columns
            media_df = pd.DataFrame(media_stats_list)
            df = pd.concat([df, media_df], axis=1)
        
        # Save to CSV
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        csv_file = os.path.join(OUTPUT_DIR, f"accounts_with_media_{TIMESTAMP}.csv")
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"  ✓ Exported accounts with media to {csv_file} ({len(df)} rows)")
        return True
    
    except Exception as e:
        print(f"  ✗ Error exporting accounts with media: {e}")
        return False


def export_table_to_csv(table_name: str, conn):
    """Export a single table or view to a CSV file."""
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        csv_file = os.path.join(OUTPUT_DIR, f"{table_name}_{TIMESTAMP}.csv")
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"  ✓ Exported {table_name} to {csv_file} ({len(df)} rows)")
        return True
    
    except Exception as e:
        print(f"  ✗ Error exporting {table_name}: {e}")
        return False


def export_mongodb_collections(mongo_db):
    """Export key MongoDB collections to CSV."""
    if not mongo_db:
        print("⚠ Skipping MongoDB exports (no connection)")
        return
    
    print("\n📊 Exporting MongoDB Collections:")
    
    collections = [
        'execution_sessions',
        'video_recordings',
        'screenshot_metadata',
        'weekly_workflow_schedules',
        'dag_execution_log'
    ]
    
    for collection_name in collections:
        try:
            collection = mongo_db[collection_name]
            cursor = collection.find()
            
            # Convert to list of dicts
            data = []
            for doc in cursor:
                # Convert ObjectId to string
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                if 'gridfs_file_id' in doc:
                    doc['gridfs_file_id'] = str(doc['gridfs_file_id'])
                data.append(doc)
            
            if data:
                df = pd.DataFrame(data)
                csv_file = os.path.join(OUTPUT_DIR, f"mongo_{collection_name}_{TIMESTAMP}.csv")
                df.to_csv(csv_file, index=False, encoding='utf-8')
                print(f"  ✓ Exported {collection_name} to {csv_file} ({len(df)} rows)")
            else:
                print(f"  ⚠ {collection_name} is empty")
        
        except Exception as e:
            print(f"  ✗ Error exporting {collection_name}: {e}")


def create_media_summary_report(mongo_db):
    """Create a summary report of all media across accounts."""
    if not mongo_db:
        return
    
    print("\n📈 Creating Media Summary Report:")
    
    try:
        # Get all accounts from MongoDB video_recordings
        pipeline = [
            {'$group': {
                '_id': '$postgres_account_id',
                'username': {'$first': '$account_username'},
                'profile_id': {'$first': '$profile_id'},
                'total_sessions': {'$sum': 1},
                'total_workflows': {'$sum': '$workflows_executed'},
                'total_screenshots': {'$sum': '$screenshot_count'},
                'completed_videos': {
                    '$sum': {'$cond': [{'$eq': ['$video_recording_status', 'completed']}, 1, 0]}
                },
                'total_duration_hours': {'$sum': {'$divide': ['$execution_duration_seconds', 3600]}},
                'last_session': {'$max': '$created_at'}
            }},
            {'$sort': {'total_sessions': -1}}
        ]
        
        results = list(mongo_db.video_recordings.aggregate(pipeline))
        
        if results:
            df = pd.DataFrame(results)
            df.rename(columns={'_id': 'account_id'}, inplace=True)
            
            csv_file = os.path.join(OUTPUT_DIR, f"media_summary_report_{TIMESTAMP}.csv")
            df.to_csv(csv_file, index=False, encoding='utf-8')
            
            print(f"  ✓ Created media summary report: {csv_file}")
            
            # Print summary statistics
            print(f"\n  Summary Statistics:")
            print(f"    Total Accounts with Media: {len(df)}")
            print(f"    Total Sessions: {df['total_sessions'].sum()}")
            print(f"    Total Workflows: {df['total_workflows'].sum()}")
            print(f"    Total Screenshots: {df['total_screenshots'].sum()}")
            print(f"    Total Completed Videos: {df['completed_videos'].sum()}")
            print(f"    Total Execution Time: {df['total_duration_hours'].sum():.2f} hours")
        else:
            print("  ⚠ No media data found")
    
    except Exception as e:
        print(f"  ✗ Error creating media summary: {e}")


def main():
    """Main export function."""
    print("=" * 70)
    print("PostgreSQL & MongoDB Data Export with Media Linkage")
    print("=" * 70)
    
    # Connect to databases
    try:
        pg_conn = psycopg2.connect(**DB_PARAMS)
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")
        return
    
    mongo_db = get_mongodb_connection()
    
    print("\n📋 Exporting PostgreSQL Tables:")
    
    # Export accounts with media statistics (special handling)
    export_accounts_with_media(pg_conn, mongo_db)
    
    # Export other core tables
    for table in CORE_TABLES:
        if table != 'accounts':  # Already exported with media stats
            export_table_to_csv(table, pg_conn)
    
    print("\n📊 Exporting PostgreSQL Views:")
    for view in VIEWS:
        export_table_to_csv(view, pg_conn)
    
    # Export MongoDB collections
    export_mongodb_collections(mongo_db)
    
    # Create media summary report
    create_media_summary_report(mongo_db)
    
    # Close connections
    pg_conn.close()
    print("\n✓ PostgreSQL connection closed")
    
    print("\n" + "=" * 70)
    print(f"✓ Export completed successfully!")
    print(f"  Output directory: {OUTPUT_DIR}")
    print(f"  Timestamp: {TIMESTAMP}")
    print("=" * 70)


if __name__ == "__main__":
    main()