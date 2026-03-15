import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from pymongo import MongoClient
from .connection import get_mongo_collection

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS FOR MONGODB ACCESS
# ============================================================================
def _get_mongo_client():
    """Get MongoDB client connection."""
    return MongoClient(
        os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
        serverSelectionTimeoutMS=5000
    )


def _get_metadata_collection():
    """Get workflow_metadata collection from messages_db."""
    client = _get_mongo_client()
    messages_db = client['messages_db']
    return messages_db['workflow_metadata'], client


class WorkflowStatsManager:
    """Professional workflow statistics manager for MongoDB collections."""

    def __init__(self, available_categories):
        self.stats_collection = get_mongo_collection("automa_workflows")
        self.metadata_collection = get_mongo_collection("workflow_metadata")
        self.execution_collection = get_mongo_collection("execution_workflows")
        self.available_categories = available_categories

    def get_category_stats(self, category: str) -> Dict[str, Any]:
        """Get comprehensive statistics for a specific category."""
        try:
            # Get collections in this category
            collections = self._get_collections_by_category(category)

            # Get workflow types in this category
            workflow_types = self._get_workflow_types_by_category(category)

            # Get execution stats
            execution_stats = self._get_category_execution_stats(category)

            # Get workflow stats
            workflow_stats = self._get_category_workflow_stats(category)

            return {
                'category': category,
                'total_collections': len(collections),
                'collections': collections,
                'workflow_types': workflow_types,
                'total_workflows': workflow_stats.get('total_workflows', 0),
                'executed_workflows': execution_stats.get('executed_workflows', 0),
                'successful_executions': execution_stats.get('successful_executions', 0),
                'failed_executions': execution_stats.get('failed_executions', 0),
                'workflows_with_content': workflow_stats.get('workflows_with_content', 0),
                'workflows_with_link': workflow_stats.get('workflows_with_link', 0),
                'avg_execution_time_ms': execution_stats.get('avg_execution_time_ms', 0),
                'avg_generation_time_ms': execution_stats.get('avg_generation_time_ms', 0)
            }

        except Exception as e:
            logger.error(f"Error fetching stats for category '{category}': {e}")
            return self._get_empty_category_stats(category)

    def _get_collections_by_category(self, category: str) -> List[Dict[str, Any]]:
        """Get all collections for a category from workflow_metadata."""
        try:
            metadata_collection, client = _get_metadata_collection()

            pipeline = [
                {"$match": {"category": category.lower()}},
                {
                    "$group": {
                        "_id": {
                            "database": "$database_name",
                            "collection": "$collection_name",
                            "workflow_type": "$workflow_type"
                        },
                        "count": {"$sum": 1},
                        "executed_count": {"$sum": {"$cond": [{"$eq": ["$executed", True]}, 1, 0]}},
                        "success_count": {"$sum": {"$cond": [{"$eq": ["$success", True]}, 1, 0]}},
                        "last_execution": {"$max": "$executed_at"}
                    }
                },
                {"$sort": {"_id.workflow_type": 1, "_id.collection": 1}}
            ]

            results = list(metadata_collection.aggregate(pipeline))
            client.close()

            collections = []
            for result in results:
                collections.append({
                    'database': result['_id']['database'],
                    'name': result['_id']['collection'],
                    'workflow_type': result['_id']['workflow_type'],
                    'total_workflows': result['count'],
                    'executed': result['executed_count'],
                    'successful': result['success_count'],
                    'last_execution': result['last_execution']
                })

            return collections

        except Exception as e:
            logger.error(f"Error fetching collections for category '{category}': {e}")
            return []

    def _get_workflow_types_by_category(self, category: str) -> List[str]:
        """Get workflow types for a category from workflow_metadata."""
        try:
            metadata_collection, client = _get_metadata_collection()

            pipeline = [
                {"$match": {"category": category.lower()}},
                {"$group": {"_id": "$workflow_type"}},
                {"$sort": {"_id": 1}}
            ]

            results = list(metadata_collection.aggregate(pipeline))
            client.close()

            return [result['_id'] for result in results]

        except Exception as e:
            logger.error(f"Error fetching workflow types for category '{category}': {e}")
            return []

    def _get_category_execution_stats(self, category: str) -> Dict[str, Any]:
        """Get execution statistics for a category from workflow_metadata."""
        try:
            metadata_collection, client = _get_metadata_collection()

            pipeline = [
                {"$match": {"category": category.lower()}},
                {
                    "$facet": {
                        "execution_stats": [
                            {"$match": {"executed": True}},
                            {
                                "$group": {
                                    "_id": None,
                                    "total_executed": {"$sum": 1},
                                    "total_successful": {"$sum": {"$cond": [{"$eq": ["$success", True]}, 1, 0]}},
                                    "total_failed": {"$sum": {"$cond": [{"$eq": ["$success", False]}, 1, 0]}},
                                    "avg_execution_time": {"$avg": "$execution_time_ms"},
                                    "avg_generation_time": {"$avg": "$generation_time_ms"}
                                }
                            }
                        ]
                    }
                }
            ]

            results = list(metadata_collection.aggregate(pipeline))
            client.close()

            if results and results[0]['execution_stats']:
                stats = results[0]['execution_stats'][0]
                return {
                    'executed_workflows': stats['total_executed'],
                    'successful_executions': stats['total_successful'],
                    'failed_executions': stats['total_failed'],
                    'avg_execution_time_ms': round(stats.get('avg_execution_time', 0), 2),
                    'avg_generation_time_ms': round(stats.get('avg_generation_time', 0), 2)
                }

            return {
                'executed_workflows': 0,
                'successful_executions': 0,
                'failed_executions': 0,
                'avg_execution_time_ms': 0,
                'avg_generation_time_ms': 0
            }

        except Exception as e:
            logger.error(f"Error fetching execution stats for category '{category}': {e}")
            return {
                'executed_workflows': 0,
                'successful_executions': 0,
                'failed_executions': 0,
                'avg_execution_time_ms': 0,
                'avg_generation_time_ms': 0
            }

    def _get_category_workflow_stats(self, category: str) -> Dict[str, Any]:
        """Get workflow statistics for a category from workflow_metadata."""
        try:
            metadata_collection, client = _get_metadata_collection()

            pipeline = [
                {"$match": {"category": category.lower()}},
                {
                    "$group": {
                        "_id": None,
                        "total_workflows": {"$sum": 1},
                        # FIX: Check for actual_content field instead of just has_content flag
                        "with_actual_content": {
                            "$sum": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$ne": ["$actual_content", None]},
                                            {"$ne": ["$actual_content", ""]},
                                            {"$gt": [{"$strLenCP": {"$ifNull": ["$actual_content", ""]}}, 0]}
                                        ]
                                    },
                                    1, 0
                                ]
                            }
                        },
                        # FIX: Check for link_url or associated_link_url instead of just has_link flag
                        "with_link_url": {
                            "$sum": {
                                "$cond": [
                                    {
                                        "$or": [
                                            {"$and": [
                                                {"$ne": ["$link_url", None]},
                                                {"$ne": ["$link_url", ""]},
                                                {"$gt": [{"$strLenCP": {"$ifNull": ["$link_url", ""]}}, 0]}
                                            ]},
                                            {"$and": [
                                                {"$ne": ["$associated_link_url", None]},
                                                {"$ne": ["$associated_link_url", ""]},
                                                {"$gt": [{"$strLenCP": {"$ifNull": ["$associated_link_url", ""]}}, 0]}
                                            ]}
                                        ]
                                    },
                                    1, 0
                                ]
                            }
                        },
                        # For compatibility with old has_content/has_link fields
                        "with_content_flag": {
                            "$sum": {"$cond": [{"$eq": ["$has_content", True]}, 1, 0]}
                        },
                        "with_link_flag": {
                            "$sum": {"$cond": [{"$eq": ["$has_link", True]}, 1, 0]}
                        }
                    }
                }
            ]

            results = list(metadata_collection.aggregate(pipeline))
            client.close()

            if results:
                stats = results[0]

                # Determine which count to use - prefer actual data over flags
                content_count = stats['with_actual_content']
                if content_count == 0 and stats['with_content_flag'] > 0:
                    content_count = stats['with_content_flag']

                link_count = stats['with_link_url']
                if link_count == 0 and stats['with_link_flag'] > 0:
                    link_count = stats['with_link_flag']

                return {
                    'total_workflows': stats['total_workflows'],
                    'workflows_with_content': content_count,
                    'workflows_with_link': link_count,
                    'workflows_with_actual_content': stats['with_actual_content'],
                    'workflows_with_link_url': stats['with_link_url'],
                    'workflows_with_content_flag': stats['with_content_flag'],
                    'workflows_with_link_flag': stats['with_link_flag']
                }

            return {
                'total_workflows': 0,
                'workflows_with_content': 0,
                'workflows_with_link': 0,
                'workflows_with_actual_content': 0,
                'workflows_with_link_url': 0,
                'workflows_with_content_flag': 0,
                'workflows_with_link_flag': 0
            }

        except Exception as e:
            logger.error(f"Error fetching workflow stats for category '{category}': {e}")
            return {
                'total_workflows': 0,
                'workflows_with_content': 0,
                'workflows_with_link': 0,
                'workflows_with_actual_content': 0,
                'workflows_with_link_url': 0,
                'workflows_with_content_flag': 0,
                'workflows_with_link_flag': 0
            }

    def _get_empty_category_stats(self, category: str) -> Dict[str, Any]:
        """Return empty stats for a category."""
        return {
            'category': category,
            'total_collections': 0,
            'collections': [],
            'workflow_types': [],
            'total_workflows': 0,
            'executed_workflows': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'workflows_with_content': 0,
            'workflows_with_link': 0,
            'avg_execution_time_ms': 0,
            'avg_generation_time_ms': 0
        }

    def get_workflow_type_summary(self) -> Dict[str, Dict[str, int]]:
        """Get summary statistics for all categories."""
        summary = {}

        for category in self.available_categories:
            try:
                stats = self.get_category_stats(category)
                summary[category] = {
                    'total': stats.get('total_workflows', 0),
                    'executed': stats.get('executed_workflows', 0),
                    'successful': stats.get('successful_executions', 0),
                    'collections': stats.get('total_collections', 0),
                    'workflow_types': len(stats.get('workflow_types', []))
                }
            except Exception as e:
                logger.error(f"Error getting summary for category '{category}': {e}")
                summary[category] = {
                    'total': 0,
                    'executed': 0,
                    'successful': 0,
                    'collections': 0,
                    'workflow_types': 0
                }

        return summary


# ============================================================================
# REMAINING FUNCTIONS (Keep as-is, just for reference)
# ============================================================================

def get_filtered_executions_from_mongodb(filters: Dict[str, Any] = None, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches filtered workflow executions from the MongoDB workflow_metadata collection."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return []

    try:
        query = filters or {}

        projection = {
            "_id": 1,
            "automa_workflow_id": 1,
            "postgres_content_id": 1,
            "postgres_account_id": 1,
            "workflow_type": 1,
            "status": 1,
            "executed": 1,
            "success": 1,
            "generated_at": 1,
            "executed_at": 1,
            "execution_time_ms": 1,
            "blocks_generated": 1,
            "template_used": 1,
            "error_message": 1,
        }

        cursor = collection.find(query, projection).sort("generated_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)

        executions = list(cursor)
        for execution in executions:
            execution['_id'] = str(execution['_id'])
            if 'automa_workflow_id' in execution and execution['automa_workflow_id']:
                execution['automa_workflow_id'] = str(execution['automa_workflow_id'])

        logger.info(f"Retrieved {len(executions)} workflow metadata records from MongoDB.")
        return executions
    except Exception as e:
        logger.error(f"Error fetching workflow metadata from MongoDB: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error fetching workflow metadata from MongoDB: {str(e)}")
        return []

    def get_comprehensive_workflow_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """For compatibility - use category-based stats instead."""
        # This method is kept for compatibility but will return category-based stats
        # Find which category this workflow type belongs to
        for category, types in self.available_workflow_types.items():
            if workflow_type in types:
                return self.get_category_stats(category)

        # If not found, return empty
        return self._get_empty_category_stats(workflow_type)


    def _build_filter_query(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build MongoDB query from filters dictionary.

        Args:
            workflow_type: Type of workflow
            filters: Optional dictionary of filters to apply

        Returns:
            MongoDB query dictionary
        """
        query = {"workflow_type": workflow_type}

        if not filters:
            return query

        # Link filter
        if 'link' in filters:
            query["associated_link_url"] = filters['link']

        # Date filters
        if 'tweeted_date' in filters:
            query["tweeted_date"] = filters['tweeted_date'].strftime('%Y-%m-%d')

        if 'date_from' in filters and 'date_to' in filters:
            query["tweeted_date"] = {
                "$gte": filters['date_from'].strftime('%Y-%m-%d'),
                "$lte": filters['date_to'].strftime('%Y-%m-%d')
            }

        # Account filter
        if 'account_id' in filters:
            query["postgres_account_id"] = filters['account_id']

        # Execution status filter
        if 'execution_status' in filters:
            if filters['execution_status'] == 'success':
                query["success"] = True
                query["executed"] = True
            elif filters['execution_status'] == 'failed':
                query["success"] = False
                query["executed"] = True
            elif filters['execution_status'] == 'executed':
                query["executed"] = True

        # Has content filter
        if 'content_types' in filters:
            # This is mainly used for 'all' workflow type
            pass  # Already handled by workflow_type parameter

        return query


    def _get_overview_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        """Get total number of workflows for a specific workflow type with filters."""
        query = self._build_filter_query(workflow_type, filters)

        # Count unique automa_workflow_ids
        pipeline = [
            {"$match": query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result = list(self.metadata_collection.aggregate(pipeline))
        total_workflows = result[0]['total'] if result else 0

        return {'total_workflows': total_workflows}


    def _get_execution_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        """Get execution-related statistics for workflows with filters."""
        query = self._build_filter_query(workflow_type, filters)

        # Count unique workflows that have been executed
        executed_query = query.copy()
        executed_query["executed"] = True

        pipeline_executed = [
            {"$match": executed_query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result_executed = list(self.metadata_collection.aggregate(pipeline_executed))
        executed_workflows = result_executed[0]['total'] if result_executed else 0

        # Count successful executions
        success_query = query.copy()
        success_query["success"] = True
        success_query["executed"] = True

        pipeline_success = [
            {"$match": success_query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result_success = list(self.metadata_collection.aggregate(pipeline_success))
        successful_executions = result_success[0]['total'] if result_success else 0

        # Count failed executions
        failed_query = query.copy()
        failed_query["success"] = False
        failed_query["executed"] = True

        pipeline_failed = [
            {"$match": failed_query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result_failed = list(self.metadata_collection.aggregate(pipeline_failed))
        failed_executions = result_failed[0]['total'] if result_failed else 0

        return {
            'executed_workflows': executed_workflows,
            'successful_executions': successful_executions,
            'failed_executions': failed_executions,
            'not_executed_workflows': 0  # Can be calculated if needed
        }


    def _get_content_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        """Get content-related statistics with filters."""
        query = self._build_filter_query(workflow_type, filters)

        # Count workflows with content
        with_content_query = query.copy()
        with_content_query["has_content"] = True

        pipeline_with_content = [
            {"$match": with_content_query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result_with_content = list(self.metadata_collection.aggregate(pipeline_with_content))
        workflows_with_content = result_with_content[0]['total'] if result_with_content else 0

        # Count workflows without content
        without_content_query = query.copy()
        without_content_query["has_content"] = False

        pipeline_without_content = [
            {"$match": without_content_query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result_without_content = list(self.metadata_collection.aggregate(pipeline_without_content))
        workflows_without_content = result_without_content[0]['total'] if result_without_content else 0

        return {
            'workflows_with_content': workflows_with_content,
            'workflows_without_content': workflows_without_content
        }


    def _get_link_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        """Get link-related statistics from workflow_metadata with filters."""
        query = self._build_filter_query(workflow_type, filters)

        # Count workflows with link
        with_link_query = query.copy()
        with_link_query["has_link"] = True

        pipeline_with_link = [
            {"$match": with_link_query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result_with_link = list(self.metadata_collection.aggregate(pipeline_with_link))
        workflows_with_link = result_with_link[0]['total'] if result_with_link else 0

        # Count workflows without link
        without_link_query = query.copy()
        without_link_query["has_link"] = False

        pipeline_without_link = [
            {"$match": without_link_query},
            {"$group": {"_id": "$automa_workflow_id"}},
            {"$count": "total"}
        ]

        result_without_link = list(self.metadata_collection.aggregate(pipeline_without_link))
        workflows_without_link = result_without_link[0]['total'] if result_without_link else 0

        return {
            'workflows_with_link': workflows_with_link,
            'workflows_without_link': workflows_without_link
        }


    def _get_performance_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get performance-related statistics including generation time with filters."""
        query = self._build_filter_query(workflow_type, filters)

        # Add existence check for time fields
        query["$and"] = [
            {"execution_time": {"$exists": True}},
            {"generation_time": {"$exists": True}}
        ]

        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": None,
                    "avg_execution_time": {"$avg": "$execution_time"},
                    "min_execution_time": {"$min": "$execution_time"},
                    "max_execution_time": {"$max": "$execution_time"},
                    "total_execution_time": {"$sum": "$execution_time"},
                    "avg_generation_time": {"$avg": "$generation_time"},
                    "min_generation_time": {"$min": "$generation_time"},
                    "max_generation_time": {"$max": "$generation_time"},
                    "total_generation_time": {"$sum": "$generation_time"}
                }
            }
        ]

        result = list(self.metadata_collection.aggregate(pipeline))
        if result:
            perf_data = result[0]
            return {
                'average_execution_time_ms': round(perf_data.get('avg_execution_time') or 0, 2),
                'fastest_execution_ms': perf_data.get('min_execution_time') or 0,
                'slowest_execution_ms': perf_data.get('max_execution_time') or 0,
                'total_execution_time_ms': perf_data.get('total_execution_time') or 0,
                'average_generation_time_ms': round(perf_data.get('avg_generation_time') or 0, 2),
                'fastest_generation_ms': perf_data.get('min_generation_time') or 0,
                'slowest_generation_ms': perf_data.get('max_generation_time') or 0,
                'total_generation_time_ms': perf_data.get('total_generation_time') or 0
            }
        return {
            'average_execution_time_ms': 0,
            'fastest_execution_ms': 0,
            'slowest_execution_ms': 0,
            'total_execution_time_ms': 0,
            'average_generation_time_ms': 0,
            'fastest_generation_ms': 0,
            'slowest_generation_ms': 0,
            'total_generation_time_ms': 0
        }


    def _get_temporal_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get time-based statistics including generation metrics with filters."""
        from datetime import datetime, timedelta

        query = self._build_filter_query(workflow_type, filters)

        now = datetime.now()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)

        # Created today
        created_today_query = query.copy()
        created_today_query["created_at"] = {"$gte": today}

        # Created this week
        created_week_query = query.copy()
        created_week_query["created_at"] = {"$gte": week_ago}

        # Created this month
        created_month_query = query.copy()
        created_month_query["created_at"] = {"$gte": month_ago}

        # Executed today
        executed_today_query = query.copy()
        executed_today_query["started_at"] = {"$gte": today}

        # Executed this week
        executed_week_query = query.copy()
        executed_week_query["started_at"] = {"$gte": week_ago}

        # Generated today
        generated_today_query = query.copy()
        generated_today_query["generated_at"] = {"$gte": today}

        # Generated this week
        generated_week_query = query.copy()
        generated_week_query["generated_at"] = {"$gte": week_ago}

        return {
            'created_today': self.stats_collection.count_documents(created_today_query),
            'created_this_week': self.stats_collection.count_documents(created_week_query),
            'created_this_month': self.stats_collection.count_documents(created_month_query),
            'executed_today': self.metadata_collection.count_documents(executed_today_query),
            'executed_this_week': self.metadata_collection.count_documents(executed_week_query),
            'generated_today': self.metadata_collection.count_documents(generated_today_query),
            'generated_this_week': self.metadata_collection.count_documents(generated_week_query)
        }


    def get_comprehensive_workflow_stats(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetches comprehensive statistics about workflows organized by categories.

        Args:
            workflow_type: Type of workflow ('all', 'replies', 'messages', 'retweets')
            filters: Optional dictionary of filters to apply

        Returns:
            Dictionary containing organized workflow statistics
        """
        if self.stats_collection is None or self.metadata_collection is None:
            logger.error("Failed to access MongoDB collections.")
            return self._get_empty_stats()

        try:
            # Handle "all" workflow type by aggregating all three types
            if workflow_type == 'all':
                return self._get_combined_workflow_stats(filters)

            # Individual workflow type stats with filters
            stats = {
                'overview': self._get_overview_stats(workflow_type, filters),
                'execution': self._get_execution_stats(workflow_type, filters),
                'content': self._get_content_stats(workflow_type, filters),
                'link': self._get_link_stats(workflow_type, filters),
                'performance': self._get_performance_stats(workflow_type, filters),
                'temporal': self._get_temporal_stats(workflow_type, filters)
            }

            logger.info(f"Successfully retrieved comprehensive {workflow_type} workflow statistics.")
            return stats

        except Exception as e:
            logger.error(f"Error fetching {workflow_type} workflow stats: {e}")
            if STREAMLIT_AVAILABLE:
                st.error(f"❌ Error fetching {workflow_type} workflow statistics: {str(e)}")
            return self._get_empty_stats()


    def _get_combined_workflow_stats(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Combines statistics from all workflow types (replies, messages, retweets).

        Args:
            filters: Optional dictionary of filters to apply

        Returns:
            Dictionary containing aggregated workflow statistics
        """
        try:
            workflow_types = ['replies', 'messages', 'retweets']
            combined_stats = {
                'overview': {},
                'execution': {},
                'content': {},
                'link': {},
                'performance': {},
                'temporal': {}
            }

            # Initialize counters
            total_workflows = 0
            executed_workflows = 0
            successful_executions = 0
            failed_executions = 0
            workflows_with_content = 0
            workflows_without_content = 0
            workflows_with_link = 0
            workflows_without_link = 0

            # Aggregate stats from all workflow types
            for wf_type in workflow_types:
                stats = {
                    'overview': self._get_overview_stats(wf_type, filters),
                    'execution': self._get_execution_stats(wf_type, filters),
                    'content': self._get_content_stats(wf_type, filters),
                    'link': self._get_link_stats(wf_type, filters),
                }

                # Sum up the values
                total_workflows += stats['overview'].get('total_workflows', 0)
                executed_workflows += stats['execution'].get('executed_workflows', 0)
                successful_executions += stats['execution'].get('successful_executions', 0)
                failed_executions += stats['execution'].get('failed_executions', 0)
                workflows_with_content += stats['content'].get('workflows_with_content', 0)
                workflows_without_content += stats['content'].get('workflows_without_content', 0)
                workflows_with_link += stats['link'].get('workflows_with_link', 0)
                workflows_without_link += stats['link'].get('workflows_without_link', 0)

            # Build combined stats
            combined_stats['overview'] = {
                'total_workflows': total_workflows
            }

            combined_stats['execution'] = {
                'executed_workflows': executed_workflows,
                'successful_executions': successful_executions,
                'failed_executions': failed_executions,
                'success_rate': (successful_executions / executed_workflows * 100) if executed_workflows > 0 else 0
            }

            combined_stats['content'] = {
                'workflows_with_content': workflows_with_content,
                'workflows_without_content': workflows_without_content,
                'content_percentage': (workflows_with_content / total_workflows * 100) if total_workflows > 0 else 0
            }

            combined_stats['link'] = {
                'workflows_with_link': workflows_with_link,
                'workflows_without_link': workflows_without_link,
                'link_percentage': (workflows_with_link / total_workflows * 100) if total_workflows > 0 else 0
            }

            # Performance and temporal stats can be averaged or left empty
            combined_stats['performance'] = {}
            combined_stats['temporal'] = {}

            logger.info("Successfully retrieved combined workflow statistics for all types.")
            return combined_stats

        except Exception as e:
            logger.error(f"Error fetching combined workflow stats: {e}")
            if STREAMLIT_AVAILABLE:
                st.error(f"❌ Error fetching combined workflow statistics: {str(e)}")
            return self._get_empty_stats()


    def get_workflow_success_rate(self, workflow_type: str, filters: Optional[Dict[str, Any]] = None) -> float:
        """
        Calculate success rate for workflows.

        Args:
            workflow_type: Type of workflow ('all', 'replies', 'messages', 'retweets')
            filters: Optional dictionary of filters to apply

        Returns:
            Success rate as percentage
        """
        try:
            if workflow_type == 'all':
                stats = self._get_combined_workflow_stats(filters)
            else:
                stats = self.get_comprehensive_workflow_stats(workflow_type, filters)

            return stats['execution'].get('success_rate', 0)

        except Exception as e:
            logger.error(f"Error calculating success rate for {workflow_type}: {e}")
            return 0.0


    def _get_empty_stats(self) -> Dict[str, Any]:
        """
        Returns an empty statistics dictionary with proper structure.

        Returns:
            Dictionary with all required stat categories initialized to empty/zero values
        """
        return {
            'overview': {
                'total_workflows': 0
            },
            'execution': {
                'executed_workflows': 0,
                'successful_executions': 0,
                'failed_executions': 0,
                'success_rate': 0
            },
            'content': {
                'workflows_with_content': 0,
                'workflows_without_content': 0,
                'content_percentage': 0
            },
            'link': {
                'workflows_with_link': 0,
                'workflows_without_link': 0,
                'link_percentage': 0
            },
            'performance': {},
            'temporal': {}
        }

    def get_daily_execution_trend(self, workflow_type: str, days: int = 7) -> List[Dict[str, Any]]:
        """Get daily execution trends for the specified number of days."""
        try:
            end_date = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            start_date = end_date - timedelta(days=days-1)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

            pipeline = [
                {
                    "$match": {
                        "workflow_type": workflow_type,
                        "started_at": {"$gte": start_date, "$lte": end_date}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$started_at"},
                            "month": {"$month": "$started_at"},
                            "day": {"$dayOfMonth": "$started_at"}
                        },
                        "total_executions": {"$sum": 1},
                        "successful_executions": {
                            "$sum": {"$cond": [{"$eq": ["$success", True]}, 1, 0]}
                        },
                        "avg_generation_time": {"$avg": "$generation_time"}
                    }
                },
                {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1}}
            ]

            results = list(self.metadata_collection.aggregate(pipeline))
            return [
                {
                    'date': f"{result['_id']['year']}-{result['_id']['month']:02d}-{result['_id']['day']:02d}",
                    'total_executions': result['total_executions'],
                    'successful_executions': result['successful_executions'],
                    'success_rate': round((result['successful_executions'] / result['total_executions']) * 100, 2),
                    'avg_generation_time_ms': round(result.get('avg_generation_time', 0), 2)
                }
                for result in results
            ]

        except Exception as e:
            logger.error(f"Error getting daily execution trend for {workflow_type}: {e}")
            return []


# ========== ORIGINAL FUNCTIONS (Enhanced with additional fields) ==========



def create_execution_record(
    automa_workflow_id: str,
    postgres_content_id: int,
    postgres_user_id: int,
    workflow_type: str,
    status: str = "generated",
    **kwargs
) -> Optional[str]:
    """Creates a new execution record in the workflow_metadata collection."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return None

    try:
        execution_doc = {
            "automa_workflow_id": ObjectId(automa_workflow_id),
            "postgres_content_id": postgres_content_id,
            "postgres_user_id": postgres_user_id,
            "workflow_type": workflow_type,
            "status": status,
            "executed": False,
            "success": False,
            "generated_at": datetime.now().isoformat(),
            "executed_at": None,
            "started_at": None,
            "completed_at": None,
            "generation_time": kwargs.get('generation_time', 0),
            "execution_time": None,
            "actual_execution_time": None,
            "blocks_generated": kwargs.get('blocks_generated', 0),
            "template_used": kwargs.get('template_used'),
            "error_message": None,
            "execution_attempts": 0,
            "last_error_message": None,
            "last_error_timestamp": None,
            "single_workflow_execution": kwargs.get('single_workflow_execution', False),
            "processing_priority": kwargs.get('processing_priority', 1),
            "retry_count": kwargs.get('retry_count', 0),
            "chrome_session_id": kwargs.get('chrome_session_id'),
            "injection_time": kwargs.get('injection_time'),
            "trigger_time": kwargs.get('trigger_time'),
            "processing_duration": kwargs.get('processing_duration')
        }

        result = collection.insert_one(execution_doc)
        logger.info(f"Created execution record with ID: {result.inserted_id}")
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Error creating execution record: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error creating execution record: {str(e)}")
        return None

def update_execution_record(execution_id: str, updates: Dict[str, Any]):
    """Updates an execution record in the workflow_metadata collection."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return

    try:
        updates["last_updated"] = datetime.now().isoformat()

        result = collection.update_one({"_id": ObjectId(execution_id)}, {"$set": updates})
        if result.modified_count > 0:
            logger.info(f"Updated execution record {execution_id}")
        else:
            logger.warning(f"No execution record found with ID {execution_id} or no changes applied")
    except Exception as e:
        logger.error(f"Error updating execution record {execution_id}: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error updating execution record: {str(e)}")

def mark_execution_started(execution_id: str, chrome_session_id: str = None):
    """Mark an execution as started."""
    updates = {
        "status": "running",
        "started_at": datetime.now().isoformat()
    }

    if chrome_session_id:
        updates["chrome_session_id"] = chrome_session_id

    update_execution_record(execution_id, updates)

def mark_execution_completed(execution_id: str, success: bool, execution_time: int = None, error_message: str = None):
    """Mark an execution as completed."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        return

    updates = {
        "status": "completed" if success else "failed",
        "executed": True,
        "success": success,
        "completed_at": datetime.now().isoformat()
    }

    if execution_time is not None:
        updates["actual_execution_time"] = execution_time

    if error_message:
        updates["error_message"] = error_message
        updates["last_error_message"] = error_message
        updates["last_error_timestamp"] = datetime.now().isoformat()

    collection.update_one(
        {"_id": ObjectId(execution_id)},
        {
            "$set": updates,
            "$inc": {"execution_attempts": 1}
        }
    )

def get_execution_by_id(execution_id: str) -> Optional[Dict[str, Any]]:
    """Fetches an execution record by its ID."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return None

    try:
        execution = collection.find_one({"_id": ObjectId(execution_id)})
        if execution:
            execution['_id'] = str(execution['_id'])
            if 'automa_workflow_id' in execution and execution['automa_workflow_id']:
                execution['automa_workflow_id'] = str(execution['automa_workflow_id'])
            logger.info(f"Retrieved execution record {execution_id}")
            return execution
        else:
            logger.warning(f"No execution record found with ID {execution_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching execution record {execution_id}: {e}")
        return None

def get_executions_by_content_id(content_id: int, workflow_type: str = None) -> List[Dict[str, Any]]:
    """Fetches execution records by content ID."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return []

    try:
        query = {"postgres_content_id": content_id}
        if workflow_type:
            query["workflow_type"] = workflow_type

        cursor = collection.find(query).sort("generated_at", -1)
        executions = list(cursor)

        for execution in executions:
            execution['_id'] = str(execution['_id'])
            if 'automa_workflow_id' in execution and execution['automa_workflow_id']:
                execution['automa_workflow_id'] = str(execution['automa_workflow_id'])

        logger.info(f"Retrieved {len(executions)} execution records for content_id {content_id}")
        return executions
    except Exception as e:
        logger.error(f"Error fetching execution records for content_id {content_id}: {e}")
        return []

def get_pending_executions(workflow_type: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches execution records that are pending (not executed)."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return []

    try:
        query = {"executed": False}
        if workflow_type:
            query["workflow_type"] = workflow_type

        cursor = collection.find(query).sort("generated_at", 1)
        if limit:
            cursor = cursor.limit(limit)

        executions = list(cursor)
        for execution in executions:
            execution['_id'] = str(execution['_id'])
            if 'automa_workflow_id' in execution and execution['automa_workflow_id']:
                execution['automa_workflow_id'] = str(execution['automa_workflow_id'])

        logger.info(f"Retrieved {len(executions)} pending execution records")
        return executions
    except Exception as e:
        logger.error(f"Error fetching pending execution records: {e}")
        return []

def get_execution_statistics(workflow_type: str = None) -> Dict[str, Any]:
    """Get execution statistics."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return {}

    try:
        match_filter = {}
        if workflow_type:
            match_filter["workflow_type"] = workflow_type

        pipeline = [
            {"$match": match_filter},
            {"$group": {
                "_id": None,
                "total_executions": {"$sum": 1},
                "executed_count": {"$sum": {"$cond": ["$executed", 1, 0]}},
                "successful_count": {"$sum": {"$cond": ["$success", 1, 0]}},
                "failed_count": {"$sum": {"$cond": [{"$and": ["$executed", {"$eq": ["$success", False]}]}, 1, 0]}},
                "avg_generation_time": {"$avg": "$generation_time"},
                "avg_execution_time": {"$avg": "$actual_execution_time"}
            }}
        ]

        result = list(collection.aggregate(pipeline))
        if result:
            stats = result[0]
            stats.pop('_id', None)
            stats['success_rate'] = (stats['successful_count'] / stats['executed_count'] * 100) if stats['executed_count'] > 0 else 0

            logger.info(f"Retrieved execution statistics for {workflow_type or 'all'} workflows")
            return stats
        else:
            return {
                "total_executions": 0,
                "executed_count": 0,
                "successful_count": 0,
                "failed_count": 0,
                "success_rate": 0,
                "avg_generation_time": 0,
                "avg_execution_time": 0
            }
    except Exception as e:
        logger.error(f"Error fetching execution statistics: {e}")
        return {}

def reverse_js_workflow_operations(workflow_type: str = None, account_id: int = None, dag_run_id: str = None) -> dict:
    """
    Reverse all MongoDB operations performed by the JavaScript workflow orchestrator.
    This precisely undoes everything the JS updateWorkflowExecution method does, plus related operations.

    Args:
        workflow_type: Optional filter by workflow type ('replies', 'messages', 'retweets')
        account_id: Optional filter by account ID (postgres_account_id)
        dag_run_id: Optional filter by specific DAG run ID

    Returns:
        dict: Summary of what was reversed/reset
    """
    try:
        workflow_metadata_collection = get_mongo_collection("workflow_metadata")
        if workflow_metadata_collection is None:
            logger.error("Failed to access MongoDB workflow_metadata collection.")
            return {"error": "Database connection failed", "total_reversed": 0}

        if account_id is not None:
            try:
                account_id = int(account_id)
            except (ValueError, TypeError):
                logger.warning(f"Invalid account_id provided: {account_id}, skipping account filter")
                account_id = None

        reversed_counts = {
            "workflow_metadata_reset": 0,
            "account_profile_assignments_reversed": 0,
            "daily_workflow_limits_reset": 0,
            "workflow_execution_tracking_deleted": 0,
            "workflow_copies_deleted": 0,
            "execution_batches_deleted": 0,
            "browser_sessions_closed": 0,
            "recording_sessions_cleaned": 0
        }

        execution_filter = {}
        if workflow_type:
            execution_filter["workflow_type"] = workflow_type
        if account_id is not None:
            execution_filter["postgres_account_id"] = account_id
        if dag_run_id:
            execution_filter["dag_run_id"] = dag_run_id

        logger.info(f"Reversing updateWorkflowExecution operations with filters: "
                   f"workflow_type={workflow_type}, account_id={account_id}, dag_run_id={dag_run_id}")

        execution_reset_data = {
            "executed": False,
            "executed_at": None,
            "execution_success": False,
            "execution_mode": None,
            "updated_at": None,
            "postgres_account_id": None,
            "account_username": None,
            "profile_id": None,
            "profile_specific_execution": None,
            "extension_id": None,
            "video_recording_session_id": None,
            "video_recording_enabled": None,
            "dag_run_id": None,
            "final_result": None,
            "execution_time": None,
            "steps_taken": None,
            "execution_error": None,
            "error_category": None,
            "reversed_at": datetime.now(timezone.utc),
            "reversed_by": "reverse_js_workflow_operations",
            "reversal_reason": "undo_updateWorkflowExecution_operation"
        }

        test_workflow_filter = {
            "_id": {
                "$not": {
                    "$regex": "^(direct_|test_)"
                }
            }
        }
        execution_filter.update(test_workflow_filter)

        result_executions = workflow_metadata_collection.update_many(
            execution_filter,
            {"$set": execution_reset_data}
        )
        reversed_counts["workflow_metadata_reset"] = result_executions.modified_count
        logger.info(f"Reset {result_executions.modified_count} workflow metadata records (excluding test workflows)")

        profiles_collection = get_mongo_collection("account_profile_assignments")
        if profiles_collection is not None and account_id is not None:  # ✅ FIXED
            executed_records = list(workflow_metadata_collection.find(
                execution_filter,
                {"profile_id": 1, "postgres_account_id": 1, "account_username": 1}
            ))

            profiles_to_reset = set()
            for record in executed_records:
                if record.get("profile_id") and record.get("postgres_account_id"):
                    profiles_to_reset.add((record["postgres_account_id"], record["profile_id"]))

            for pg_account_id, profile_id in profiles_to_reset:
                profile_reset_data = {
                    "usage_stats.workflows_executed": 0,
                    "usage_stats.last_workflow_date": None,
                    "usage_stats.success_rate": 0.0,
                    "usage_stats.total_sessions": 0,
                    "usage_stats.last_execution_details": None,
                    "reversed_at": datetime.now(timezone.utc),
                    "reversal_reason": "undo_trackAccountProfileAssignment"
                }

                result_profile = profiles_collection.update_one(
                    {
                        "postgres_account_id": pg_account_id,
                        "profile_id": profile_id,
                        "is_active": True
                    },
                    {"$set": profile_reset_data}
                )
                reversed_counts["account_profile_assignments_reversed"] += result_profile.modified_count

        analytics_collection = get_mongo_collection("daily_workflow_analytics")
        if analytics_collection is not None:  # ✅ FIXED
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

            daily_limit_reset_data = {
                "workflows_executed": 0,
                "workflows_remaining": None,
                "limit_reached": False,
                "limit_utilization_percentage": 0.0,
                "last_execution_at": None,
                "current_count": 0,
                "daily_limit": None,
                "reversed_at": datetime.now(timezone.utc),
                "needs_recalculation": True,
                "reversal_reason": "undo_daily_workflow_limit_tracking"
            }

            date_filter = {
                "date": {
                    "$gte": today - timedelta(days=1),
                    "$lte": today
                }
            }

            if account_id is not None:
                date_filter["postgres_account_id"] = account_id

            result_analytics = analytics_collection.update_many(
                date_filter,
                {"$set": daily_limit_reset_data}
            )
            reversed_counts["daily_workflow_limits_reset"] = result_analytics.modified_count
            logger.info(f"Reset {result_analytics.modified_count} daily workflow limit tracking records")

        tracking_collection = get_mongo_collection("workflow_execution_tracking")
        if tracking_collection is not None:  # ✅ FIXED
            tracking_filter = {}
            if dag_run_id:
                tracking_filter["dag_run_id"] = dag_run_id
            if account_id is not None:
                execution_ids = list(workflow_metadata_collection.find(
                    execution_filter,
                    {"_id": 1}
                ))
                execution_id_strings = [str(exec_rec["_id"]) for exec_rec in execution_ids]
                if execution_id_strings:
                    tracking_filter["execution_id"] = {"$in": execution_id_strings}

            result_tracking = tracking_collection.delete_many(tracking_filter)
            reversed_counts["workflow_execution_tracking_deleted"] = result_tracking.deleted_count
            logger.info(f"Deleted {result_tracking.deleted_count} execution tracking records")

        copies_collection = get_mongo_collection("workflow_modified_copies")
        if copies_collection is not None:  # ✅ FIXED
            copy_filter = {}
            if workflow_type:
                copy_filter["workflow_type"] = workflow_type
            if account_id is not None:
                copy_filter["account_id"] = account_id
            if dag_run_id:
                copy_filter["created_for_dag_run"] = dag_run_id

            result_copies = copies_collection.delete_many(copy_filter)
            reversed_counts["workflow_copies_deleted"] = result_copies.deleted_count
            logger.info(f"Deleted {result_copies.deleted_count} workflow copies")

        batches_collection = get_mongo_collection("multi_type_execution_batches")
        if batches_collection is not None:  # ✅ FIXED
            batch_filter = {}
            if account_id is not None:
                batch_filter["account_id"] = account_id

            result_batches = batches_collection.delete_many(batch_filter)
            reversed_counts["execution_batches_deleted"] = result_batches.deleted_count
            logger.info(f"Deleted {result_batches.deleted_count} execution batches")

        sessions_collection = get_mongo_collection("browser_sessions")
        if sessions_collection is not None:  # ✅ FIXED
            session_filter = {
                "session_purpose": {"$in": ["account_workflow_execution", "account_specific_execution"]},
                "workflow_type": "account_specific_execution"
            }
            if account_id is not None:
                session_filter["postgres_account_id"] = account_id
            if dag_run_id:
                session_filter["dag_run_id"] = dag_run_id

            session_update_data = {
                "is_active": False,
                "session_status": "reversed",
                "ended_at": datetime.now(timezone.utc),
                "workflow_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "automa_integration_errors": 0,
                "reversed_at": datetime.now(timezone.utc),
                "reversal_reason": "undo_workflow_execution_session"
            }

            result_sessions = sessions_collection.update_many(
                session_filter,
                {"$set": session_update_data}
            )
            reversed_counts["browser_sessions_closed"] = result_sessions.modified_count
            logger.info(f"Closed {result_sessions.modified_count} browser sessions")

        collections_with_recordings = [
            "session_recordings",
            "video_recordings",
            "workflow_execution_recordings"
        ]

        total_recording_cleanup = 0
        for collection_name in collections_with_recordings:
            try:
                collection = get_mongo_collection(collection_name)
                if collection is not None:  # ✅ FIXED
                    recording_cleanup_filter = {}
                    if account_id is not None:
                        recording_cleanup_filter["accountId"] = account_id
                    if dag_run_id:
                        recording_cleanup_filter["dagRunId"] = dag_run_id
                    result = collection.delete_many(recording_cleanup_filter)
                    total_recording_cleanup += result.deleted_count
            except Exception as e:
                logger.warning(f"Could not clean {collection_name}: {e}")

        reversed_counts["recording_sessions_cleaned"] = total_recording_cleanup
        logger.info(f"Cleaned {total_recording_cleanup} recording session records")

        total_reversed = sum(reversed_counts.values())

        summary = {
            "success": True,
            "total_reversed": total_reversed,
            "details": reversed_counts,
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id,
                "dag_run_id": dag_run_id
            },
            "reversed_at": datetime.now(timezone.utc).isoformat(),
            "primary_operations_reversed": [
                "updateWorkflowExecution MongoDB updates completely undone",
                "trackAccountProfileAssignment usage statistics reset",
                "isDailyWorkflowLimitReached progress counters reset"
            ],
            "supporting_operations_reversed": [
                "workflow_execution_tracking records deleted",
                "workflow_modified_copies deleted",
                "multi_type_execution_batches deleted",
                "browser_sessions closed and reset",
                "video recording session data cleaned"
            ],
            "note": "Precisely reverses updateWorkflowExecution method and supporting operations - original templates preserved"
        }

        filter_desc = []
        if workflow_type:
            filter_desc.append(f"workflow_type={workflow_type}")
        if account_id is not None:
            filter_desc.append(f"account_id={account_id}")
        if dag_run_id:
            filter_desc.append(f"dag_run_id={dag_run_id}")
        filter_str = ", ".join(filter_desc) if filter_desc else "all updateWorkflowExecution operations"

        logger.info(f"Successfully reversed {total_reversed} updateWorkflowExecution operation records for {filter_str}")
        logger.info(f"Primary reversals: workflow_metadata={reversed_counts['workflow_metadata_reset']}, "
                   f"profile_assignments={reversed_counts['account_profile_assignments_reversed']}, "
                   f"daily_limits={reversed_counts['daily_workflow_limits_reset']}")

        return summary

    except Exception as e:
        error_msg = f"Error reversing updateWorkflowExecution operations: {e}"
        logger.error(error_msg)

        return {
            "success": False,
            "error": error_msg,
            "total_reversed": 0,
            "details": {},
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id,
                "dag_run_id": dag_run_id
            }
        }

def delete_content_workflow_links(workflow_type: str = None) -> int:
    """This function is deprecated as content_workflow_links collection no longer exists."""
    logger.warning("delete_content_workflow_links is deprecated: content_workflow_links collection does not exist.")
    return 0

def reverse_link_extraction_operations(workflow_type: str = None, account_id: int = None) -> dict:
    """
    Reverse MongoDB operations performed by the JavaScript link extraction and processing.

    Args:
        workflow_type: Optional filter by workflow type
        account_id: Optional filter by account ID

    Returns:
        dict: Summary of what was reversed/reset
    """
    try:
        reversed_counts = {
            "links_updated_deleted": 0,
            "workflow_metadata_unlinked": 0,
            "account_statistics_reset": 0
        }

        base_filter = {}
        if workflow_type:
            base_filter["workflow_type"] = workflow_type
        if account_id is not None:
            base_filter["postgres_account_id"] = account_id

        logger.info(f"Reversing link extraction operations with filters: "
                   f"workflow_type={workflow_type}, account_id={account_id}")

        links_updated_collection = get_mongo_collection("links_updated")
        if links_updated_collection is not None:  # ✅ FIXED
            links_filter = {}
            if account_id is not None:
                links_filter["postgres_account_id"] = account_id

            result_links = links_updated_collection.delete_many(links_filter)
            reversed_counts["links_updated_deleted"] = result_links.deleted_count
            logger.info(f"Deleted {result_links.deleted_count} links_updated records")

        workflow_metadata_collection = get_mongo_collection("workflow_metadata")
        if workflow_metadata_collection is not None:  # ✅ FIXED
            execution_filter = {}
            if workflow_type:
                execution_filter["workflow_type"] = workflow_type
            if account_id is not None:
                execution_filter["postgres_account_id"] = account_id

            link_reset_data = {
                "postgres_link_id": None,
                "associated_link_url": None,
                "link_tweet_id": None,
                "link_source_page": None,
                "link_association_reset_at": datetime.now(timezone.utc),
                "link_association_reset_reason": "reverse_link_extraction_operations"
            }

            result_executions = workflow_metadata_collection.update_many(
                execution_filter,
                {"$set": link_reset_data}
            )
            reversed_counts["workflow_metadata_unlinked"] = result_executions.modified_count
            logger.info(f"Reset {result_executions.modified_count} workflow metadata link associations")

        accounts_collection = get_mongo_collection("accounts")
        if accounts_collection is not None and account_id is not None:  # ✅ FIXED
            account_reset_data = {
                "total_links_processed": 0,
                "total_replies_processed": 0,
                "total_messages_processed": 0,
                "total_retweets_processed": 0,
                "last_workflow_sync": None,
                "account_statistics_reset_at": datetime.now(timezone.utc),
                "statistics_reset_reason": "reverse_link_extraction_operations"
            }

            result_accounts = accounts_collection.update_one(
                {"postgres_account_id": account_id},
                {"$set": account_reset_data}
            )
            reversed_counts["account_statistics_reset"] = result_accounts.modified_count
            logger.info(f"Reset account statistics for account_id {account_id}")

        total_reversed = sum(reversed_counts.values())

        summary = {
            "success": True,
            "total_reversed": total_reversed,
            "details": reversed_counts,
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id
            },
            "reversed_at": datetime.now(timezone.utc).isoformat(),
            "operations_reversed": [
                "MongoDB links_updated collection records deleted",
                "MongoDB workflow_metadata link associations reset",
                "MongoDB account statistics reset"
            ],
            "note": "Reverses JavaScript link extraction and database operations - PostgreSQL operations would need separate handling"
        }

        filter_desc = []
        if workflow_type:
            filter_desc.append(f"workflow_type={workflow_type}")
        if account_id is not None:
            filter_desc.append(f"account_id={account_id}")
        filter_str = ", ".join(filter_desc) if filter_desc else "all link extraction operations"

        logger.info(f"Successfully reversed {total_reversed} link extraction operation records for {filter_str}")

        return summary

    except Exception as e:
        error_msg = f"Error reversing link extraction operations: {e}"
        logger.error(error_msg)

        return {
            "success": False,
            "error": error_msg,
            "total_reversed": 0,
            "details": {},
            "filters_applied": {
                "workflow_type": workflow_type,
                "account_id": account_id
            }
        }

def delete_execution_records(workflow_type: str = None) -> int:
    """Delete execution records, optionally filtered by workflow type."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return 0

    try:
        query = {}
        if workflow_type:
            query["workflow_type"] = workflow_type

        result = collection.delete_many(query)
        deleted_count = result.deleted_count
        logger.info(f"Deleted {deleted_count} execution records from MongoDB.")
        return deleted_count
    except Exception as e:
        logger.error(f"Error deleting execution records: {e}")
        if STREAMLIT_AVAILABLE:
            st.error(f"Error deleting execution records: {str(e)}")
        return 0

# ========== NEW ENHANCED FUNCTIONS ==========

def get_executions_by_workflow_id(automa_workflow_id: str, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches executions for a specific workflow."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return []

    try:
        query = {"automa_workflow_id": ObjectId(automa_workflow_id)}
        cursor = collection.find(query).sort("generated_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)

        executions = list(cursor)
        for execution in executions:
            execution['_id'] = str(execution['_id'])
            execution['automa_workflow_id'] = str(execution['automa_workflow_id'])

        logger.info(f"Retrieved {len(executions)} executions for workflow {automa_workflow_id}")
        return executions
    except Exception as e:
        logger.error(f"Error fetching executions for workflow {automa_workflow_id}: {e}")
        return []

def get_recent_executions_trend(workflow_type: str = None, days: int = 7) -> List[Dict[str, Any]]:
    """Get recent execution trend data."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return []

    try:
        match_stage = {}
        if workflow_type:
            match_stage["workflow_type"] = workflow_type

        start_date = datetime.now() - timedelta(days=days)
        match_stage["generated_at"] = {"$gte": start_date.isoformat()}

        pipeline = [
            {"$match": match_stage},
            {
                "$addFields": {
                    "generated_date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {"$dateFromString": {"dateString": "$generated_at"}}
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$generated_date",
                    "total_executions": {"$sum": 1},
                    "executed_count": {"$sum": {"$cond": [{"$eq": ["$executed", True]}, 1, 0]}},
                    "successful_count": {"$sum": {"$cond": [{"$eq": ["$success", True]}, 1, 0]}},
                    "failed_count": {"$sum": {"$cond": [{"$and": [{"$eq": ["$executed", True]}, {"$eq": ["$success", False]}]}, 1, 0]}}
                }
            },
            {"$sort": {"_id": 1}}
        ]

        result = list(collection.aggregate(pipeline))

        trend_data = []
        for item in result:
            trend_data.append({
                "date": item["_id"],
                "total_executions": item["total_executions"],
                "executed": item["executed_count"],
                "successful": item["successful_count"],
                "failed": item["failed_count"]
            })

        return trend_data
    except Exception as e:
        logger.error(f"Error fetching execution trend: {e}")
        return []

def update_execution_status(execution_id: str, status_updates: Dict[str, Any]) -> bool:
    """Update execution status and related fields."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return False

    try:
        status_updates["last_updated"] = datetime.now().isoformat()

        result = collection.update_one(
            {"_id": ObjectId(execution_id)},
            {"$set": status_updates}
        )

        if result.modified_count > 0:
            logger.info(f"Updated execution {execution_id}")
            return True
        else:
            logger.warning(f"No execution found with ID {execution_id} or no changes applied")
            return False
    except Exception as e:
        logger.error(f"Error updating execution {execution_id}: {e}")
        return False

def get_unexecuted_workflows(workflow_type: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Get unexecuted workflows for processing (single workflow processing compatible)."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return []

    try:
        query = {"executed": False}
        if workflow_type:
            query["workflow_type"] = workflow_type

        cursor = collection.find(query).sort([
            ("processing_priority", 1),
            ("generated_at", 1)
        ])

        if limit is not None:
            cursor = cursor.limit(limit)

        executions = list(cursor)
        for execution in executions:
            execution['_id'] = str(execution['_id'])
            if execution.get('automa_workflow_id'):
                execution['automa_workflow_id'] = str(execution['automa_workflow_id'])

        logger.info(f"Retrieved {len(executions)} unexecuted workflows")
        return executions
    except Exception as e:
        logger.error(f"Error fetching unexecuted workflows: {e}")
        return []

def get_execution_logs_enhanced(execution_id: str = None, workflow_id: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Fetches enhanced execution logs."""
    collection = get_mongo_collection("workflow_logs_enhanced")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_logs_enhanced collection.")
        return []

    try:
        query = {}
        if execution_id:
            query["execution_id"] = execution_id
        if workflow_id:
            query["workflow_id"] = workflow_id

        cursor = collection.find(query).sort("timestamp", -1)
        if limit is not None:
            cursor = cursor.limit(limit)

        logs = list(cursor)
        logger.info(f"Retrieved {len(logs)} enhanced execution logs")
        return logs
    except Exception as e:
        logger.error(f"Error fetching enhanced execution logs: {e}")
        return []

def get_single_workflow_performance_metrics(workflow_type: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Get performance metrics from single workflow processing."""
    collection = get_mongo_collection("single_workflow_performance")
    if collection is None:
        logger.warning("single_workflow_performance collection not available")
        return []

    try:
        query = {}
        if workflow_type:
            query["workflow_type"] = workflow_type

        cursor = collection.find(query).sort("measured_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)

        metrics = list(cursor)
        for metric in metrics:
            metric['_id'] = str(metric['_id'])
            if metric.get('automa_workflow_id'):
                metric['automa_workflow_id'] = str(metric['automa_workflow_id'])

        logger.info(f"Retrieved {len(metrics)} single workflow performance metrics")
        return metrics
    except Exception as e:
        logger.error(f"Error fetching single workflow performance metrics: {e}")
        return []

def get_single_workflow_execution_logs(workflow_type: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Get single workflow execution logs."""
    collection = get_mongo_collection("single_workflow_execution_log")
    if collection is None:
        logger.warning("single_workflow_execution_log collection not available")
        return []

    try:
        query = {}
        if workflow_type:
            query["workflow_type"] = workflow_type

        cursor = collection.find(query).sort("started_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)

        logs = list(cursor)
        for log in logs:
            log['_id'] = str(log['_id'])
            if log.get('automa_workflow_id'):
                log['automa_workflow_id'] = str(log['automa_workflow_id'])

        logger.info(f"Retrieved {len(logs)} single workflow execution logs")
        return logs
    except Exception as e:
        logger.error(f"Error fetching single workflow execution logs: {e}")
        return []

def delete_execution_by_id(execution_id: str) -> bool:
    """Delete a specific execution record."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return False

    try:
        result = collection.delete_one({"_id": ObjectId(execution_id)})
        if result.deleted_count > 0:
            logger.info(f"Deleted execution {execution_id}")
            return True
        else:
            logger.warning(f"No execution found with ID {execution_id}")
            return False
    except Exception as e:
        logger.error(f"Error deleting execution {execution_id}: {e}")
        return False

def mark_single_workflow_injection(execution_id: str, injection_time: str, chrome_session_id: str = None):
    """Mark when a single workflow was injected into Chrome."""
    updates = {
        "injection_time": injection_time,
        "single_workflow_execution": True,
        "status": "injected"
    }

    if chrome_session_id:
        updates["chrome_session_id"] = chrome_session_id

    update_execution_record(execution_id, updates)

def mark_single_workflow_triggered(execution_id: str, trigger_time: str):
    """Mark when a single workflow trigger was sent."""
    updates = {
        "trigger_time": trigger_time,
        "status": "triggered"
    }

    update_execution_record(execution_id, updates)

def get_oldest_unexecuted_workflow(workflow_type: str = None) -> Optional[Dict[str, Any]]:
    """Get the oldest unexecuted workflow for single workflow processing."""
    executions = get_unexecuted_workflows(workflow_type=workflow_type, limit=1)
    return executions[0] if executions else None

def increment_retry_count(execution_id: str):
    """Increment the retry count for a workflow execution."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is not None:  # ✅ FIXED
        try:
            collection.update_one(
                {"_id": ObjectId(execution_id)},
                {"$inc": {"retry_count": 1}}
            )
            logger.info(f"Incremented retry count for execution {execution_id}")
        except Exception as e:
            logger.error(f"Error incrementing retry count for execution {execution_id}: {e}")

def set_processing_priority(execution_id: str, priority: int):
    """Set processing priority for a workflow execution (1 = highest priority)."""
    updates = {"processing_priority": priority}
    update_execution_record(execution_id, updates)

def get_executions_by_priority(workflow_type: str = None, priority: int = None, limit: int = None) -> List[Dict[str, Any]]:
    """Get executions filtered by priority level."""
    collection = get_mongo_collection("workflow_metadata")
    if collection is None:
        logger.error("Failed to access MongoDB workflow_metadata collection.")
        return []

    try:
        query = {"executed": False}
        if workflow_type:
            query["workflow_type"] = workflow_type
        if priority is not None:
            query["processing_priority"] = priority

        cursor = collection.find(query).sort([
            ("processing_priority", 1),
            ("generated_at", 1)
        ])

        if limit is not None:
            cursor = cursor.limit(limit)

        executions = list(cursor)
        for execution in executions:
            execution['_id'] = str(execution['_id'])
            if execution.get('automa_workflow_id'):
                execution['automa_workflow_id'] = str(execution['automa_workflow_id'])

        logger.info(f"Retrieved {len(executions)} executions by priority")
        return executions
    except Exception as e:
        logger.error(f"Error fetching executions by priority: {e}")
        return []
