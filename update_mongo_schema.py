#!/usr/bin/env python3
"""
MongoDB Schema Update Script for GridFS Screenshots and Video Recordings
Adds support for:
- GridFS-based screenshot storage
- Video recording metadata with profile linkage
- Enhanced execution session tracking
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import os

# Configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@localhost:27017/messages_db?authSource=admin')
DB_NAME = os.getenv('MONGODB_DB_NAME', 'messages_db')

def main():
    print('=' * 70)
    print('MongoDB Schema Update: GridFS Screenshots & Video Recordings')
    print('=' * 70)
    
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    
    try:
        # 1. Create screenshot_metadata collection
        create_screenshot_metadata_collection(db)
        
        # 2. Create video_recordings collection
        create_video_recordings_collection(db)
        
        # 3. Update execution_sessions collection
        update_execution_sessions_collection(db)
        
        # 4. Create indexes for performance
        create_indexes(db)
        
        # 5. Create views for querying
        create_views(db)
        
        # 6. Update system settings
        update_system_settings(db)
        
        print('\n' + '=' * 70)
        print('✓ Schema update completed successfully!')
        print('=' * 70)
        
        # Print summary
        print_summary(db)
        
    except Exception as e:
        print(f'\n❌ Error during schema update: {e}')
        raise
    finally:
        client.close()


def create_screenshot_metadata_collection(db):
    """Create screenshot_metadata collection for GridFS file tracking"""
    print('\n1. Creating screenshot_metadata collection...')
    
    # Drop existing if it exists
    if 'screenshot_metadata' in db.list_collection_names():
        db.screenshot_metadata.drop()
        print('   Dropped existing screenshot_metadata collection')
    
    # Create with validator
    db.create_collection('screenshot_metadata', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['gridfs_file_id', 'session_id', 'category', 'created_at'],
            'properties': {
                'gridfs_file_id': {
                    'bsonType': 'objectId',
                    'description': 'GridFS file ID reference'
                },
                'session_id': {
                    'bsonType': 'string',
                    'description': 'Hyperbrowser session ID'
                },
                'postgres_account_id': {
                    'bsonType': 'int',
                    'description': 'PostgreSQL account ID'
                },
                'account_username': {
                    'bsonType': 'string',
                    'description': 'Account username'
                },
                'profile_id': {
                    'bsonType': ['string', 'null'],
                    'description': 'Hyperbrowser profile ID'
                },
                'workflow_id': {
                    'bsonType': ['string', 'null'],
                    'description': 'Workflow ID if applicable'
                },
                'category': {
                    'bsonType': 'string',
                    'enum': ['preExecution', 'postExecution', 'errors', 'debug'],
                    'description': 'Screenshot category'
                },
                'filename': {
                    'bsonType': 'string',
                    'description': 'Original filename'
                },
                'contentType': {
                    'bsonType': 'string',
                    'description': 'MIME type (image/png)'
                },
                'size': {
                    'bsonType': 'int',
                    'description': 'File size in bytes'
                },
                'created_at': {
                    'bsonType': 'date',
                    'description': 'Upload timestamp'
                }
            }
        }
    })
    
    print('   ✓ screenshot_metadata collection created')


def create_video_recordings_collection(db):
    """Create video_recordings collection for video metadata"""
    print('\n2. Creating video_recordings collection...')
    
    # Drop existing if it exists
    if 'video_recordings' in db.list_collection_names():
        db.video_recordings.drop()
        print('   Dropped existing video_recordings collection')
    
    # Create with validator
    db.create_collection('video_recordings', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['session_id', 'profile_id', 'postgres_account_id', 'created_at'],
            'properties': {
                'session_id': {
                    'bsonType': 'string',
                    'description': 'Hyperbrowser session ID'
                },
                'profile_id': {
                    'bsonType': 'string',
                    'description': 'Hyperbrowser profile ID'
                },
                'postgres_account_id': {
                    'bsonType': 'int',
                    'description': 'PostgreSQL account ID'
                },
                'account_username': {
                    'bsonType': 'string',
                    'description': 'Account username'
                },
                'execution_session_id': {
                    'bsonType': ['objectId', 'null'],
                    'description': 'Reference to execution_sessions'
                },
                'web_recording_url': {
                    'bsonType': ['string', 'null'],
                    'description': 'URL to rrweb JSON recording'
                },
                'web_recording_status': {
                    'bsonType': 'string',
                    'enum': ['not_enabled', 'pending', 'in_progress', 'completed', 'failed'],
                    'description': 'Web recording status'
                },
                'video_recording_url': {
                    'bsonType': ['string', 'null'],
                    'description': 'URL to MP4 video recording'
                },
                'video_recording_status': {
                    'bsonType': 'string',
                    'enum': ['not_enabled', 'pending', 'in_progress', 'completed', 'failed'],
                    'description': 'Video recording status'
                },
                'recording_error': {
                    'bsonType': ['string', 'null'],
                    'description': 'Error message if failed'
                },
                'screenshot_count': {
                    'bsonType': 'int',
                    'description': 'Number of screenshots taken'
                },
                'screenshot_file_ids': {
                    'bsonType': 'array',
                    'items': {'bsonType': 'objectId'},
                    'description': 'Array of GridFS file IDs'
                },
                'workflows_executed': {
                    'bsonType': 'int',
                    'description': 'Number of workflows in this session'
                },
                'execution_duration_seconds': {
                    'bsonType': 'int',
                    'description': 'Total execution time'
                },
                'created_at': {
                    'bsonType': 'date',
                    'description': 'Session creation time'
                },
                'updated_at': {
                    'bsonType': 'date',
                    'description': 'Last update time'
                },
                'completed_at': {
                    'bsonType': ['date', 'null'],
                    'description': 'Session completion time'
                }
            }
        }
    })
    
    print('   ✓ video_recordings collection created')


def update_execution_sessions_collection(db):
    """Update execution_sessions with enhanced video and screenshot fields"""
    print('\n3. Updating execution_sessions collection schema...')
    
    # Update all existing documents with new fields
    result = db.execution_sessions.update_many(
        {},
        {
            '$set': {
                'web_recording_enabled': False,
                'web_recording_url': None,
                'web_recording_status': 'not_enabled',
                'video_recording_enabled': False,
                'video_recording_url': None,
                'video_recording_status': 'not_enabled',
                'screenshot_count': 0,
                'screenshot_file_ids': [],
                'updated_at': datetime.utcnow()
            }
        }
    )
    
    print(f'   ✓ Updated {result.modified_count} execution_sessions documents')


def create_indexes(db):
    """Create all necessary indexes"""
    print('\n4. Creating indexes...')
    
    # screenshot_metadata indexes
    db.screenshot_metadata.create_index([('gridfs_file_id', ASCENDING)], unique=True)
    db.screenshot_metadata.create_index([('session_id', ASCENDING)])
    db.screenshot_metadata.create_index([('postgres_account_id', ASCENDING), ('created_at', DESCENDING)])
    db.screenshot_metadata.create_index([('profile_id', ASCENDING), ('created_at', DESCENDING)])
    db.screenshot_metadata.create_index([('category', ASCENDING)])
    db.screenshot_metadata.create_index([('created_at', DESCENDING)])
    print('   ✓ screenshot_metadata indexes created')
    
    # video_recordings indexes
    db.video_recordings.create_index([('session_id', ASCENDING)], unique=True)
    db.video_recordings.create_index([('profile_id', ASCENDING), ('created_at', DESCENDING)])
    db.video_recordings.create_index([('postgres_account_id', ASCENDING), ('created_at', DESCENDING)])
    db.video_recordings.create_index([('execution_session_id', ASCENDING)])
    db.video_recordings.create_index([('web_recording_status', ASCENDING)])
    db.video_recordings.create_index([('video_recording_status', ASCENDING)])
    db.video_recordings.create_index([('created_at', DESCENDING)])
    print('   ✓ video_recordings indexes created')
    
    # execution_sessions additional indexes
    db.execution_sessions.create_index([('profile_id', ASCENDING), ('video_recording_enabled', ASCENDING)])
    db.execution_sessions.create_index([('web_recording_url', ASCENDING)])
    db.execution_sessions.create_index([('video_recording_url', ASCENDING)])
    db.execution_sessions.create_index([('screenshot_count', DESCENDING)])
    print('   ✓ execution_sessions indexes created')


def create_views(db):
    """Create MongoDB views for easy querying"""
    print('\n5. Creating views...')
    
    # View: Profile media summary
    try:
        db.command('drop', 'profile_media_summary')
    except:
        pass
    
    db.command('create', 'profile_media_summary', viewOn='video_recordings', pipeline=[
        {
            '$group': {
                '_id': '$profile_id',
                'profile_id': {'$first': '$profile_id'},
                'total_sessions': {'$sum': 1},
                'total_screenshots': {'$sum': '$screenshot_count'},
                'total_workflows': {'$sum': '$workflows_executed'},
                'completed_web_recordings': {
                    '$sum': {
                        '$cond': [{'$eq': ['$web_recording_status', 'completed']}, 1, 0]
                    }
                },
                'completed_video_recordings': {
                    '$sum': {
                        '$cond': [{'$eq': ['$video_recording_status', 'completed']}, 1, 0]
                    }
                },
                'last_session': {'$max': '$created_at'},
                'accounts_used': {'$addToSet': '$postgres_account_id'}
            }
        },
        {
            '$addFields': {
                'account_count': {'$size': '$accounts_used'}
            }
        },
        {
            '$sort': {'total_sessions': -1}
        }
    ])
    print('   ✓ profile_media_summary view created')
    
    # View: Account media tracking
    try:
        db.command('drop', 'account_media_tracking')
    except:
        pass
    
    db.command('create', 'account_media_tracking', viewOn='video_recordings', pipeline=[
        {
            '$group': {
                '_id': '$postgres_account_id',
                'postgres_account_id': {'$first': '$postgres_account_id'},
                'account_username': {'$first': '$account_username'},
                'total_sessions': {'$sum': 1},
                'total_screenshots': {'$sum': '$screenshot_count'},
                'total_workflows': {'$sum': '$workflows_executed'},
                'profiles_used': {'$addToSet': '$profile_id'},
                'web_recordings_completed': {
                    '$sum': {
                        '$cond': [{'$eq': ['$web_recording_status', 'completed']}, 1, 0]
                    }
                },
                'video_recordings_completed': {
                    '$sum': {
                        '$cond': [{'$eq': ['$video_recording_status', 'completed']}, 1, 0]
                    }
                },
                'last_session': {'$max': '$created_at'}
            }
        },
        {
            '$addFields': {
                'profile_count': {'$size': '$profiles_used'}
            }
        },
        {
            '$sort': {'total_sessions': -1}
        }
    ])
    print('   ✓ account_media_tracking view created')
    
    # View: Screenshot catalog
    try:
        db.command('drop', 'screenshot_catalog')
    except:
        pass
    
    db.command('create', 'screenshot_catalog', viewOn='screenshot_metadata', pipeline=[
        {
            '$lookup': {
                'from': 'execution_sessions',
                'localField': 'session_id',
                'foreignField': 'session_id',
                'as': 'session_info'
            }
        },
        {
            '$addFields': {
                'execution_day': {'$arrayElemAt': ['$session_info.execution_day', 0]},
                'workflows_executed': {'$arrayElemAt': ['$session_info.workflows_executed', 0]}
            }
        },
        {
            '$project': {
                'gridfs_file_id': 1,
                'session_id': 1,
                'postgres_account_id': 1,
                'account_username': 1,
                'profile_id': 1,
                'category': 1,
                'filename': 1,
                'size': 1,
                'created_at': 1,
                'execution_day': 1,
                'workflows_executed': 1
            }
        },
        {
            '$sort': {'created_at': -1}
        }
    ])
    print('   ✓ screenshot_catalog view created')


def update_system_settings(db):
    """Update system settings for media management"""
    print('\n6. Updating system settings...')
    
    result = db.settings.update_one(
        {'category': 'system'},
        {
            '$set': {
                'settings.media_management': {
                    'screenshot_storage': 'gridfs',
                    'screenshot_compression': True,
                    'screenshot_retention_days': 90,
                    'video_storage': 'hyperbrowser_cloud',
                    'video_retention_days': 30,
                    'auto_cleanup_enabled': True,
                    'max_screenshot_size_mb': 5,
                    'screenshot_categories': ['preExecution', 'postExecution', 'errors', 'debug'],
                    'enable_video_recording': True,
                    'enable_web_recording': True,
                    'recording_quality': 'high'
                },
                'settings.gridfs_configuration': {
                    'chunk_size_kb': 255,
                    'bucket_name': 'screenshots',
                    'enable_compression': True,
                    'content_types': ['image/png', 'image/jpeg']
                },
                'updated_at': datetime.utcnow()
            }
        },
        upsert=True
    )
    
    print(f'   ✓ System settings updated')


def print_summary(db):
    """Print summary of changes"""
    print('\n' + '=' * 70)
    print('Summary:')
    print('=' * 70)
    
    collections = ['screenshot_metadata', 'video_recordings', 'execution_sessions']
    for coll in collections:
        count = db[coll].count_documents({})
        indexes = len(db[coll].index_information())
        print(f'  {coll}:')
        print(f'    Documents: {count}')
        print(f'    Indexes: {indexes}')
    
    print('\nViews created:')
    print('  - profile_media_summary')
    print('  - account_media_tracking')
    print('  - screenshot_catalog')
    
    print('\nGridFS Buckets:')
    print('  - screenshots.files (auto-created by GridFS)')
    print('  - screenshots.chunks (auto-created by GridFS)')
    
    print('\nFeatures enabled:')
    print('  ✓ GridFS screenshot storage')
    print('  ✓ Video recording metadata tracking')
    print('  ✓ Profile-based media linkage')
    print('  ✓ Account-specific media queries')
    print('  ✓ Session-based media grouping')


if __name__ == '__main__':
    main()