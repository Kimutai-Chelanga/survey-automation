#!/usr/bin/env python3
"""
Simple MongoDB Workflow Exporter
Works from outside Docker using localhost connection.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict
from pymongo import MongoClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleWorkflowExporter:
    def __init__(self):
        # Use localhost connection directly since we're running from outside Docker
        self.mongodb_uri = "mongodb://admin:admin123@localhost:27017/messages_db?authSource=admin"
        self.client = None
        self.db = None
        self.export_dir = Path(f"workflow_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

    def connect(self):
        """Connect to MongoDB"""
        try:
            logger.info("Connecting to MongoDB at localhost:27017...")
            self.client = MongoClient(self.mongodb_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client['messages_db']
            logger.info("Successfully connected to MongoDB")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def discover_collections(self):
        """Find all collections and their counts"""
        collections_info = {}
        try:
            all_collections = self.db.list_collection_names()
            logger.info(f"Found {len(all_collections)} collections in database")
            
            for collection_name in all_collections:
                count = self.db[collection_name].count_documents({})
                collections_info[collection_name] = count
                logger.info(f"  {collection_name}: {count} documents")
                
        except Exception as e:
            logger.error(f"Error discovering collections: {e}")
            
        return collections_info

    def export_collection(self, collection_name, count):
        """Export a single collection"""
        if count == 0:
            logger.info(f"Skipping empty collection: {collection_name}")
            return 0

        try:
            collection = self.db[collection_name]
            collection_dir = self.export_dir / collection_name
            collection_dir.mkdir(parents=True, exist_ok=True)
            
            # Export all documents
            documents = list(collection.find({}))
            
            # Save as single JSON file
            output_file = collection_dir / f"{collection_name}_all.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, default=str, ensure_ascii=False)
            
            # Also save individual files for workflows if it's the automa_workflows collection
            if collection_name == 'automa_workflows':
                for i, doc in enumerate(documents):
                    doc_id = str(doc.get('_id', f'doc_{i}'))
                    workflow_name = doc.get('name', 'unnamed')
                    
                    individual_file = collection_dir / f"{doc_id}_{workflow_name}.json"
                    with open(individual_file, 'w', encoding='utf-8') as f:
                        json.dump(doc, f, indent=2, default=str, ensure_ascii=False)
            
            logger.info(f"Exported {len(documents)} documents from {collection_name}")
            return len(documents)
            
        except Exception as e:
            logger.error(f"Error exporting {collection_name}: {e}")
            return 0

    def create_summary_report(self, collections_info, export_results):
        """Create a summary report"""
        summary = {
            "export_timestamp": datetime.now().isoformat(),
            "mongodb_connection": "localhost:27017",
            "database": "messages_db",
            "total_collections": len(collections_info),
            "collections_info": collections_info,
            "export_results": export_results,
            "total_documents_exported": sum(export_results.values())
        }
        
        summary_file = self.export_dir / "export_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
            
        return summary

    def export_all(self):
        """Main export function"""
        if not self.connect():
            return {"error": "Connection failed"}

        # Create export directory
        self.export_dir.mkdir(exist_ok=True)
        logger.info(f"Created export directory: {self.export_dir}")

        # Discover collections
        collections_info = self.discover_collections()
        
        if not collections_info:
            logger.warning("No collections found in database")
            return {"error": "No collections found"}

        # Export each collection
        export_results = {}
        for collection_name, count in collections_info.items():
            exported_count = self.export_collection(collection_name, count)
            export_results[collection_name] = exported_count

        # Create summary
        summary = self.create_summary_report(collections_info, export_results)
        
        # Close connection
        if self.client:
            self.client.close()

        logger.info(f"Export completed! Check directory: {self.export_dir}")
        return summary

def main():
    logger.info("Starting MongoDB export...")
    
    exporter = SimpleWorkflowExporter()
    result = exporter.export_all()
    
    if "error" in result:
        logger.error(f"Export failed: {result['error']}")
        return 1

    # Print summary
    print("\n" + "="*50)
    print("EXPORT COMPLETED SUCCESSFULLY")
    print("="*50)
    print(f"Export directory: {result.get('total_documents_exported', 0)} documents exported")
    print(f"Total documents: {result.get('total_documents_exported', 0)}")
    print(f"Collections exported:")
    
    for collection, count in result.get('export_results', {}).items():
        print(f"  - {collection}: {count} documents")
    
    print(f"\nFiles saved to: {exporter.export_dir}")
    print("="*50)
    
    return 0

if __name__ == "__main__":
    exit(main())