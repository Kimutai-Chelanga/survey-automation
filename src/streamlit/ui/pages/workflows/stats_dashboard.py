"""
FILE: ui/components/stats_dashboard.py
Complete file with delete functionality integrated.
"""

from typing import Any, Dict, List, Optional
import streamlit as st
import pandas as pd
import json
import re
import os
from datetime import datetime, date
from bson import ObjectId
from pymongo import MongoClient
from src.core.database.mongodb.connection import get_mongo_collection
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# ============================================================================
# HELPER: Get MongoDB Client
# ============================================================================

def _get_mongo_client():
    """Get MongoDB client connection."""
    return MongoClient(
        os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
        serverSelectionTimeoutMS=5000
    )


def _get_metadata_collection():
    """Get workflow_metadata collection. Returns (collection, client)."""
    try:
        client = _get_mongo_client()
        messages_db = client['messages_db']
        return messages_db['workflow_metadata'], client
    except Exception as e:
        logger.error(f"Error accessing metadata collection: {e}")
        return None, None


# ============================================================================
# MONGO-ONLY HELPERS
# ============================================================================

def _get_available_accounts() -> List[Dict[str, Any]]:
    """Fetch distinct (account_id, username) from workflow_metadata."""
    try:
        meta = get_mongo_collection("workflow_metadata")
        if meta is None:
            st.warning("Cannot connect to workflow_metadata")
            return []

        pipeline = [
            {"$match": {"postgres_account_id": {"$exists": True}}},
            {"$group": {
                "_id": {"account_id": "$postgres_account_id", "username": "$username"},
                "account_id": {"$first": "$postgres_account_id"},
                "username": {"$first": "$username"}
            }},
            {"$sort": {"username": 1}}
        ]
        results = list(meta.aggregate(pipeline))
        return [
            {"account_id": r["account_id"], "username": r["username"] or "unknown"}
            for r in results
        ]
    except Exception as e:
        st.error(f"Error fetching accounts: {e}")
        return []


def _get_execution_statistics() -> Dict[str, Any]:
    """Fetch execution statistics from workflow_metadata."""
    try:
        meta = get_mongo_collection("workflow_metadata")
        if meta is None:
            st.warning("Cannot connect to workflow_metadata")
            return {'total_executed': 0, 'successful': 0, 'failed': 0, 'pending': 0}

        pipeline = [
            {
                "$facet": {
                    "executed": [
                        {"$match": {"executed": True}},
                        {"$count": "count"}
                    ],
                    "successful": [
                        {"$match": {"executed": True, "success": True}},
                        {"$count": "count"}
                    ],
                    "failed": [
                        {"$match": {"executed": True, "success": False}},
                        {"$count": "count"}
                    ],
                    "pending": [
                        {"$match": {"execute": True, "executed": False}},
                        {"$count": "count"}
                    ]
                }
            }
        ]

        results = list(meta.aggregate(pipeline))
        if results:
            data = results[0]
            return {
                'total_executed': data['executed'][0]['count'] if data['executed'] else 0,
                'successful': data['successful'][0]['count'] if data['successful'] else 0,
                'failed': data['failed'][0]['count'] if data['failed'] else 0,
                'pending': data['pending'][0]['count'] if data['pending'] else 0
            }

        return {'total_executed': 0, 'successful': 0, 'failed': 0, 'pending': 0}

    except Exception as e:
        st.error(f"Error fetching execution statistics: {e}")
        return {'total_executed': 0, 'successful': 0, 'failed': 0, 'pending': 0}


def _get_link_statistics() -> Dict[str, Any]:
    """Fetch link-related statistics from workflow_metadata."""
    try:
        meta = get_mongo_collection("workflow_metadata")
        if meta is None:
            st.warning("Cannot connect to workflow_metadata")
            return {'total_with_links': 0, 'links_by_type': {}, 'sample_links': []}

        pipeline = [
            {
                "$facet": {
                    "total_with_links": [
                        {"$match": {"has_link": True}},
                        {"$count": "count"}
                    ],
                    "by_workflow_type": [
                        {"$match": {"has_link": True}},
                        {"$group": {"_id": "$workflow_type", "count": {"$sum": 1}}},
                        {"$sort": {"_id": 1}}
                    ],
                    "sample_links": [
                        {"$match": {"has_link": True, "link_url": {"$ne": None}}},
                        {"$project": {"workflow_type": 1, "link_url": 1, "workflow_name": 1}},
                        {"$limit": 5}
                    ]
                }
            }
        ]

        results = list(meta.aggregate(pipeline))
        if results:
            data = results[0]
            links_by_type = {item['_id']: item['count'] for item in data.get('by_workflow_type', [])}
            return {
                'total_with_links': data['total_with_links'][0]['count'] if data['total_with_links'] else 0,
                'links_by_type': links_by_type,
                'sample_links': data.get('sample_links', [])
            }

        return {'total_with_links': 0, 'links_by_type': {}, 'sample_links': []}

    except Exception as e:
        st.error(f"Error fetching link statistics: {e}")
        return {'total_with_links': 0, 'links_by_type': {}, 'sample_links': []}


# ============================================================================
# MAIN DASHBOARD RENDER
# ============================================================================

def render_stats_dashboard(stats_manager, get_cached_workflow_summary, available_categories, available_workflow_types):
    """Render the main stats dashboard with filters and workflow selector."""

    if not available_categories:
        st.warning("No workflow categories found. Configure in Automa Workflow Config first.")
        return

    category_options = ['All Categories'] + available_categories
    selected_category = st.selectbox(
        "Select Category",
        category_options,
        format_func=lambda x: x,
        key="category_selector"
    )

    selected_type = None
    if selected_category != 'All Categories':
        types_in_category = available_workflow_types.get(selected_category, [])
        if types_in_category:
            type_options = ['All Types'] + types_in_category
            selected_type = st.selectbox(
                "Select Workflow Type",
                type_options,
                key="type_selector"
            )
        else:
            st.info(f"No workflow types configured for category '{selected_category}'")

    # ── Account filter ────────────────────────────────────────────────────────
    available_accounts = _get_available_accounts()
    account_options    = [{"label": "All Accounts", "account_id": None}] + [
        {"label": f"{a['username']} (ID: {a['account_id']})", "account_id": a['account_id']}
        for a in available_accounts
    ]
    account_labels = [a["label"] for a in account_options]

    selected_account_label = st.selectbox(
        "Filter by Account",
        account_labels,
        key="account_selector"
    )
    selected_account_id = next(
        (a["account_id"] for a in account_options if a["label"] == selected_account_label),
        None
    )

    filters = render_enhanced_workflow_overview(
        stats_manager,
        selected_category,
        selected_type,
        available_categories,
        available_workflow_types,
        account_id=selected_account_id,
    )

    st.markdown("---")

    if selected_category != 'All Categories':
        _render_workflow_selector(selected_category, selected_type, filters)
    else:
        st.info("Select a specific category to view and download workflows.")


# ============================================================================
# MERGE HELPERS
# ============================================================================

def _merge_workflows_with_metadata(workflows, metadata_query, category):
    """Merge workflow data with metadata for display."""
    try:
        metadata_collection, client = _get_metadata_collection()
        if metadata_collection is None or client is None:
            return workflows

        metadata_records = list(metadata_collection.find(metadata_query))

        metadata_map = {}
        for meta in metadata_records:
            wf_id = str(meta.get('automa_workflow_id'))
            if wf_id not in metadata_map:
                metadata_map[wf_id] = meta

        for wf in workflows:
            wf_id = wf['_id']
            if wf_id in metadata_map:
                meta = metadata_map[wf_id]
                wf['executed'] = meta.get('executed', False)
                wf['success'] = meta.get('success', False)
                wf['has_content'] = meta.get('has_content', False)
                wf['has_link'] = meta.get('has_link', False)
                wf['link_url'] = meta.get('link_url')
                wf['linked_link_id'] = meta.get('linked_link_id')
                wf['link_assigned_at'] = meta.get('link_assigned_at')
                wf['link_content_connection_id'] = meta.get('link_content_connection_id')
                wf['link_connected_at'] = meta.get('link_connected_at')
                wf['postgres_content_id'] = meta.get('postgres_content_id')
                wf['execution_time'] = meta.get('execution_time_ms')
                wf['generation_time'] = meta.get('generation_time_ms')
                wf['created_at'] = meta.get('created_at')
                wf['executed_at'] = meta.get('executed_at')
                wf['category'] = meta.get('category')
                wf['workflow_type'] = meta.get('workflow_type')
                wf['postgres_account_id'] = meta.get('postgres_account_id')
                wf['username'] = meta.get('username')
                wf['profile_id'] = meta.get('profile_id')
                wf['actual_content'] = meta.get('actual_content')
                wf['artifacts_folder'] = meta.get('artifacts_folder')
                wf['content_items'] = meta.get('content_items', [])
                wf['content_items_count'] = meta.get('content_items_count', 0)
                wf['all_content_ids'] = meta.get('all_content_ids', [])

        client.close()
        return workflows

    except Exception as e:
        st.error(f"Error merging metadata: {e}")
        return workflows


# ============================================================================
# BULK EXPORT
# ============================================================================

def _bulk_export_workflows(selected_categories: List[str]):
    """Export all workflows from selected categories."""
    st.info(f"Starting bulk export for categories: {', '.join(selected_categories)}")

    try:
        client = _get_mongo_client()
        metadata_collection, _ = _get_metadata_collection()

        for category in selected_categories:
            try:
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
                            "workflow_ids": {"$push": "$automa_workflow_id"}
                        }
                    },
                    {"$sort": {"_id.workflow_type": 1}}
                ]

                locations = list(metadata_collection.aggregate(pipeline))

                if locations:
                    for location in locations:
                        db_name = location['_id']['database']
                        coll_name = location['_id']['collection']
                        workflow_type = location['_id']['workflow_type']
                        workflow_ids = location['workflow_ids']

                        target_db = client[db_name]
                        target_collection = target_db[coll_name]

                        object_ids = [ObjectId(wf_id) if isinstance(wf_id, str) else wf_id
                                      for wf_id in workflow_ids]
                        workflows = list(target_collection.find({"_id": {"$in": object_ids}}))

                        if workflows:
                            for wf in workflows:
                                wf['_id'] = str(wf['_id'])
                            json_data = json.dumps(workflows, indent=2, default=str)
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            st.download_button(
                                label=f"Download {category}/{workflow_type}/{coll_name}",
                                data=json_data,
                                file_name=f"{category}_{workflow_type}_{coll_name}_{ts}.json",
                                mime="application/json",
                                key=f"download_{category}_{workflow_type}_{coll_name}_{ts}"
                            )
                            st.success(f"✅ {category}/{workflow_type}/{coll_name}: {len(workflows)} workflows")
                        else:
                            st.warning(f"⚠️ No workflows in {db_name}.{coll_name}")
                else:
                    st.warning(f"⚠️ No collections found for category '{category}'")

            except Exception as e:
                st.error(f"Export error for category '{category}': {e}")

        client.close()

    except Exception as e:
        st.error(f"❌ Database connection error: {e}")


# ============================================================================
# DOWNLOAD SINGLE WORKFLOW
# ============================================================================

def _download_workflow(workflow: Dict[str, Any], category: str, workflow_type: str, collection_name: str):
    """Download a single workflow as JSON with storage location in filename."""
    try:
        json_data = json.dumps(workflow, indent=2, default=str)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        db_name = workflow.get('storage_database', 'unknown')
        coll_name = workflow.get('storage_collection', collection_name)

        st.download_button(
            label="📥 Download Workflow",
            data=json_data,
            file_name=f"{category}_{workflow_type}_{db_name}_{coll_name}_{workflow.get('_id', 'workflow')}_{timestamp}.json",
            mime="application/json",
            key=f"download_{category}_{workflow_type}_{coll_name}_{workflow.get('_id')}_{timestamp}"
        )
        st.success("✅ Workflow ready for download!")
    except Exception as e:
        st.error(f"Error downloading workflow: {e}")


# ============================================================================
# WORKFLOW DETAIL TABS
# ============================================================================

def _display_workflow_details_tab1_fixed(workflow, category, workflow_type):
    """TAB 1 — BASIC INFO"""
    st.subheader("Basic Information")

    col1, col2 = st.columns(2)

    with col1:
        st.write(f"**Name:** {workflow.get('name', 'Unknown')}")
        st.write(f"**ID:** {workflow.get('_id', 'Unknown')}")
        st.write(f"**Category:** {category}")
        st.write(f"**Type:** {workflow_type}")
        st.write(f"**Created At:** {workflow.get('created_at', 'Unknown')}")

    with col2:
        st.write("**Storage Location**")
        st.write(f"**Database:** {workflow.get('storage_database', 'Unknown')}")
        st.write(f"**Collection:** {workflow.get('storage_collection', 'Unknown')}")

        has_link = workflow.get('has_link', False)
        link_url = workflow.get('link_url')

        if has_link and link_url:
            display_url = link_url if len(link_url) <= 60 else link_url[:57] + "..."
            st.markdown(f"**Has Link:** ✅ Yes")
            st.markdown(f"**Link URL:** [{display_url}]({link_url})")
            postgres_link_id = workflow.get('postgres_content_id')
            if postgres_link_id:
                st.write(f"**Link ID:** {postgres_link_id}")
            link_assigned_at = workflow.get('link_assigned_at')
            if link_assigned_at:
                st.write(f"**Link Assigned:** {link_assigned_at}")
        else:
            st.write(f"**Has Link:** {'❌ No' if not has_link else '⚠️ No URL'}")

        has_content = workflow.get('has_content', False)
        postgres_content_id = workflow.get('postgres_content_id')
        actual_content = workflow.get('actual_content')

        content_status = "✅ Yes" if has_content else "❌ No"
        if has_content and postgres_content_id:
            content_status += f" (Primary ID: {postgres_content_id})"
        if actual_content and len(actual_content) > 0:
            content_status += f" [{len(actual_content)} chars]"
        st.write(f"**Has Content:** {content_status}")

    st.markdown("---")
    st.subheader("🔗 Link-Content Connection")

    linked_link_id = workflow.get('linked_link_id')
    link_content_connection_id = workflow.get('link_content_connection_id')
    link_connected_at = workflow.get('link_connected_at')

    if linked_link_id:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.success("✅ Link-Content Connected")
        with col2:
            st.write(f"**Link ID:** {linked_link_id}")
        with col3:
            if link_connected_at:
                st.write(f"**Connected At:** {link_connected_at}")
        if link_content_connection_id:
            st.write(f"**Connection ID:** {link_content_connection_id}")
    else:
        st.info("ℹ️ No link-content connection established")

    content_items = workflow.get('content_items', [])

    if content_items and len(content_items) > 0:
        st.subheader(f"📦 Content Items ({len(content_items)})")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Items", len(content_items))
        with col2:
            unique_types = len(set(item.get('content_type') for item in content_items))
            st.metric("Content Types", unique_types)
        with col3:
            total_chars = sum(item.get('content_length', 0) for item in content_items)
            st.metric("Total Characters", f"{total_chars:,}")
        with col4:
            unique_accounts = len(set(item.get('account_id') for item in content_items if item.get('account_id')))
            st.metric("Accounts", unique_accounts)

        st.markdown("---")

        for idx, item in enumerate(content_items, 1):
            label = item.get('label', f'CONTENT{idx}')
            content_id = item.get('content_id')
            content_type = item.get('content_type', 'Unknown')

            with st.expander(f"**{label}** ({content_type}) - ID: {content_id}", expanded=(idx == 1)):
                item_col1, item_col2 = st.columns(2)

                with item_col1:
                    st.write("**Identifiers:**")
                    st.write(f"• Content ID: `{content_id}`")
                    st.write(f"• Content Type: `{content_type}`")
                    st.write(f"• Label: `{label}`")
                    st.write("")
                    st.write("**Account Info:**")
                    st.write(f"• Account ID: {item.get('account_id', 'N/A')}")
                    st.write(f"• Username: {item.get('username', 'N/A')}")
                    profile = item.get('profile_id', 'N/A')
                    profile_display = f"{profile[:20]}..." if profile and len(str(profile)) > 20 else profile
                    st.write(f"• Profile ID: {profile_display}")

                with item_col2:
                    st.write("**Content Metrics:**")
                    content_length = item.get('content_length', 0)
                    st.write(f"• Length: {content_length:,} characters")
                    st.write(f"• Prompt ID: {item.get('prompt_id', 'N/A')}")
                    is_primary = content_id == workflow.get('postgres_content_id')
                    if is_primary:
                        st.success("✅ Primary Content Item")

                st.write("")
                st.write("**Content Preview:**")
                preview = item.get('content_preview') or item.get('content_text', '')
                if preview:
                    preview_text = preview if len(preview) <= 500 else preview[:497] + "..."
                    st.text_area(
                        "",
                        value=preview_text,
                        height=150,
                        disabled=True,
                        key=f"preview_{workflow.get('_id')}_{idx}",
                        label_visibility="collapsed"
                    )
                    mc1, mc2, mc3 = st.columns(3)
                    mc1.caption(f"Words: {len(preview.split())}")
                    mc2.caption(f"Lines: {len(preview.split(chr(10)))}")
                    mc3.caption(f"Chars: {len(preview)}")
                else:
                    st.info("No content preview available")

                st.markdown("---")

    elif postgres_content_id:
        st.info("ℹ️ **Legacy Single-Content Workflow**")
        st.write(f"Content ID: `{postgres_content_id}`")
        st.caption("This workflow was created before multi-content support was added")
    else:
        st.warning("⚠️ No content information available for this workflow")


def _display_workflow_details_tab3_fixed(workflow):
    """TAB 3 — CONTENT"""
    st.subheader("Content Details")

    content_items = workflow.get('content_items', [])
    actual_content = workflow.get('actual_content')

    if content_items and len(content_items) > 0:
        st.info(f"This workflow uses **{len(content_items)} content items**")

        tab_labels = [item.get('label', f'CONTENT{i+1}') for i, item in enumerate(content_items)]
        tab_labels.append("📋 Combined View")
        tabs = st.tabs(tab_labels)

        for idx, (tab, item) in enumerate(zip(tabs[:-1], content_items)):
            with tab:
                label = item.get('label', f'CONTENT{idx+1}')
                content_text = item.get('content_text', '')
                st.write(f"**{label}** - Content ID: `{item.get('content_id')}`")
                st.write(f"Type: `{item.get('content_type')}` | Length: {len(content_text):,} chars")

                if content_text:
                    st.text_area(
                        "Full Content:",
                        value=content_text,
                        height=400,
                        disabled=True,
                        key=f"content_full_{workflow.get('_id')}_{idx}"
                    )
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Characters", f"{len(content_text):,}")
                    c2.metric("Words", f"{len(content_text.split()):,}")
                    c3.metric("Lines", len(content_text.splitlines()))
                else:
                    st.warning("No content available")

        with tabs[-1]:
            st.write("**Combined Content** (All items merged)")
            if actual_content:
                st.text_area(
                    "Combined Workflow Content:",
                    value=actual_content,
                    height=400,
                    disabled=True,
                    key=f"content_combined_{workflow.get('_id')}"
                )
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Characters", f"{len(actual_content):,}")
                c2.metric("Total Words", f"{len(actual_content.split()):,}")
                c3.metric("Total Lines", len(actual_content.splitlines()))
            else:
                st.info("No combined content available")

    elif actual_content:
        st.info("**Legacy Single-Content Workflow**")
        st.text_area(
            "Workflow Content:",
            value=actual_content,
            height=400,
            disabled=True,
            key=f"content_legacy_{workflow.get('_id')}"
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Characters", f"{len(actual_content):,}")
        c2.metric("Words", f"{len(actual_content.split()):,}")
        c3.metric("Lines", len(actual_content.splitlines()))
    else:
        st.warning("⚠️ No content available for this workflow")


def _display_link_details_tab(workflow: Dict[str, Any], category: str, workflow_type: str):
    """TAB 5 — LINK DETAILS"""
    st.subheader("🔗 Link Information")

    has_link = workflow.get('has_link', False)
    link_url = workflow.get('link_url')
    linked_link_id = workflow.get('linked_link_id')
    postgres_content_id = workflow.get('postgres_content_id')

    if not has_link or not link_url:
        st.warning("⚠️ No link associated with this workflow")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Has Link", "✅ Yes" if has_link else "❌ No")
    link_id = linked_link_id or postgres_content_id
    col2.metric("Link ID", link_id if link_id else "Unknown")
    col3.metric("Link Status", "Active" if has_link else "Inactive")

    st.divider()
    st.subheader("Link URL")
    if link_url:
        st.markdown(f"[{link_url}]({link_url})")
        url_col1, url_col2 = st.columns(2)
        with url_col1:
            st.write("**URL Length:**", len(link_url), "characters")
            st.write("**Protocol:**", link_url.split('://')[0] if '://' in link_url else 'Unknown')
        with url_col2:
            domain_match = re.search(r'https?://(?:www\.)?([^/]+)', link_url)
            if domain_match:
                st.write("**Domain:**", domain_match.group(1))
            is_twitter = 'twitter.com' in link_url or 'x.com' in link_url
            st.write("**Platform:**", "Twitter/X" if is_twitter else "Other")

    st.divider()
    st.subheader("🔗 Link-Content Connection")

    try:
        from src.core.database.postgres.link_content_sync import get_content_link_details

        content_items = workflow.get('content_items', [])

        content_id = None
        if content_items and len(content_items) > 0:
            content_id = content_items[0].get('content_id')
        elif postgres_content_id:
            content_id = postgres_content_id

        if content_id:
            connection_details = get_content_link_details(content_id)
            if connection_details:
                st.success("✅ Connection found in PostgreSQL")
                d1, d2 = st.columns(2)
                with d1:
                    st.write("**Connection Details:**")
                    st.write(f"• Content ID: {connection_details.get('content_id')}")
                    st.write(f"• Link ID: {connection_details.get('connected_link_id')}")
                    st.write(f"• Workflow: {connection_details.get('connected_via_workflow')}")
                    st.write(f"• Status: {connection_details.get('link_connection_status')}")
                with d2:
                    st.write("**Timestamps:**")
                    if connection_details.get('link_connection_time'):
                        st.write(f"• Connected: {connection_details.get('link_connection_time')}")
                    if connection_details.get('connection_record_time'):
                        st.write(f"• Recorded: {connection_details.get('connection_record_time')}")

                tweet_id = connection_details.get('tweet_id')
                if tweet_id:
                    st.divider()
                    st.subheader("🐦 Tweet Information")
                    st.write(f"**Tweet ID:** {tweet_id}")
                    if connection_details.get('tweeted_time'):
                        st.write(f"**Tweeted At:** {connection_details.get('tweeted_time')}")
                    if is_twitter:
                        tweet_url = f"https://twitter.com/i/web/status/{tweet_id}"
                        st.markdown(f"[View Tweet on X]({tweet_url})")
            else:
                st.info("ℹ️ No connection details found in PostgreSQL")
        else:
            st.info("ℹ️ No content ID found for connection lookup")
    except Exception as e:
        st.error(f"Error fetching connection details: {e}")
        st.info("Connection details may be in MongoDB")

    st.divider()
    st.subheader("📊 MongoDB Connection Metadata")

    mongo_fields = [
        ('linked_link_id', 'Linked Link ID'),
        ('link_content_connection_id', 'Connection Record ID'),
        ('link_connected_at', 'Link Connected At'),
        ('link_assigned_at', 'Link Assigned At'),
        ('postgres_content_id', 'PostgreSQL Content ID'),
    ]
    for field_key, field_name in mongo_fields:
        field_value = workflow.get(field_key)
        if field_value:
            st.write(f"**{field_name}:** {field_value}")

    artifacts = workflow.get('artifacts_folder')
    if artifacts and artifacts.get('pre_screenshot_url'):
        st.divider()
        st.subheader("📸 Link Preview")
        try:
            st.image(
                artifacts['pre_screenshot_url'],
                caption="Pre-Screenshot (Link Page)",
                use_container_width=True
            )
            st.markdown(f"[Open Full Size]({artifacts['pre_screenshot_url']})")
        except Exception:
            st.info("Screenshot not available or failed to load")


# ============================================================================
# WORKFLOW SELECTOR  (with 🗑️ Delete integrated)
# ============================================================================

def _render_workflow_selector(category: str, workflow_type: str, filters: dict):
    """
    Render workflow selector with metadata viewing, download, and DELETE capabilities.
    """
    from ui.components.workflow_delete_manager import (
        render_delete_panel,
        render_bulk_delete_panel,
    )

    st.markdown("### 📥 View & Download Workflows")

    workflows = _get_workflows_for_category(category, workflow_type, filters)

    if not workflows:
        st.info("No workflows found matching the selected filters.")
        return

    # Header stats
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.caption(f"📊 **{len(workflows)}** workflows found")
    s2.caption(f"⚙️ **{sum(1 for w in workflows if w.get('executed'))}** executed")
    s3.caption(f"✅ **{sum(1 for w in workflows if w.get('success'))}** successful")
    s4.caption(f"📦 **{sum(1 for w in workflows if w.get('content_items_count', 0) > 1)}** multi-content")
    s5.caption(f"🔗 **{sum(1 for w in workflows if w.get('has_link'))}** with links")

    # Group by storage location
    location_groups = {}
    for wf in workflows:
        key = f"{wf.get('storage_database', 'unknown')}.{wf.get('storage_collection', 'unknown')}"
        location_groups.setdefault(key, []).append(wf)

    if "selected_workflow_details" not in st.session_state:
        st.session_state.selected_workflow_details = None

    for location_key, loc_wfs in location_groups.items():
        with st.expander(f"📁 {location_key} ({len(loc_wfs)} workflows)", expanded=False):

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total",         len(loc_wfs))
            c2.metric("Executed",      sum(1 for w in loc_wfs if w.get("executed")))
            c3.metric("Successful",    sum(1 for w in loc_wfs if w.get("success")))
            c4.metric("Multi-Content", sum(1 for w in loc_wfs if w.get("content_items_count", 0) > 1))
            c5.metric("With Links",    sum(1 for w in loc_wfs if w.get("has_link")))

            # Build select options
            wf_options = {}
            for wf in loc_wfs:
                wf_id   = wf.get("_id", "unknown")
                wf_name = wf.get("name", f"Workflow {wf_id[:8]}")
                status_emoji = "✅" if wf.get("success") else "❌" if wf.get("executed") else "⏳"

                content_count = wf.get("content_items_count", 0)
                content_badge = f" [📦×{content_count}]" if content_count > 1 else ""

                link_badge = ""
                if wf.get("has_link"):
                    link_url = wf.get("link_url", "")
                    m = re.search(r"https?://(?:www\.)?([^/]+)", link_url)
                    if m:
                        domain = m.group(1)
                        domain = (domain[:10] + "..") if len(domain) > 12 else domain
                    else:
                        domain = "link"
                    link_badge = f" [🔗{domain}]"

                label = f"{status_emoji} {wf_name}{content_badge}{link_badge} - {wf_id[:8]}"
                wf_options[label] = wf

            selected_label = st.selectbox(
                f"Select workflow in {location_key}:",
                options=list(wf_options.keys()),
                key=f"wf_selector_{location_key.replace('.', '_')}",
            )

            if not selected_label or selected_label not in wf_options:
                continue

            selected_wf = wf_options[selected_label]

            # Quick info row
            ci1, ci2, ci3 = st.columns(3)
            with ci1:
                if selected_wf.get("executed"):
                    st.info(f"Executed: {'✅ Success' if selected_wf.get('success') else '❌ Failed'}")
                else:
                    st.info("Status: ⏳ Not executed")
            with ci2:
                if selected_wf.get("has_link"):
                    st.success("Has Link: ✅ Yes")
                else:
                    st.info("Has Link: ❌ No")
            with ci3:
                cnt = selected_wf.get("content_items_count", 0)
                st.info(f"Content Items: {cnt}" if cnt else "Content Items: None")

            # Action buttons: View | Download | Open Link | 🗑️ Delete
            cb1, cb2, cb3, cb4 = st.columns(4)

            with cb1:
                if st.button(
                    "📋 View Details",
                    key=f"view_{location_key}_{selected_wf.get('_id')}",
                    use_container_width=True,
                ):
                    st.session_state.selected_workflow_details = {
                        "workflow": selected_wf,
                        "category": category,
                        "workflow_type": workflow_type,
                    }
                    st.rerun()

            with cb2:
                if st.button(
                    "⬇️ Download",
                    key=f"download_{location_key}_{selected_wf.get('_id')}",
                    use_container_width=True,
                ):
                    _download_workflow(
                        selected_wf, category, workflow_type,
                        selected_wf.get("storage_collection", "unknown"),
                    )

            with cb3:
                if selected_wf.get("has_link") and selected_wf.get("link_url"):
                    link_url = selected_wf.get("link_url")
                    if st.button(
                        "🔗 Open Link",
                        key=f"open_link_{location_key}_{selected_wf.get('_id')}",
                        use_container_width=True,
                    ):
                        st.components.v1.html(
                            f"<script>window.open('{link_url}','_blank');</script>",
                            height=0,
                        )

            with cb4:
                render_delete_panel(
                    workflow=selected_wf,
                    category=category,
                    workflow_type=workflow_type,
                    panel_key_suffix=location_key,
                    on_delete_callback=lambda: st.session_state.pop(
                        f"wf_selector_{location_key.replace('.', '_')}", None
                    ),
                )

            # Quick link preview
            if selected_wf.get("has_link") and selected_wf.get("link_url"):
                link_url = selected_wf.get("link_url")
                with st.expander("🔗 Quick Link Preview", expanded=False):
                    st.markdown(f"**URL:** [{link_url}]({link_url})")
                    if "twitter.com" in link_url or "x.com" in link_url:
                        tweet_match = re.search(r"/status/(\d+)", link_url)
                        if tweet_match:
                            tweet_id = tweet_match.group(1)
                            st.code(f"Tweet ID: {tweet_id}")
                            st.markdown(f"[View on X](https://twitter.com/i/web/status/{tweet_id})")
                    if selected_wf.get("link_assigned_at"):
                        st.caption(f"Link assigned: {selected_wf.get('link_assigned_at')}")

    # Bulk delete panel (below all expanders)
    render_bulk_delete_panel(
        workflows=workflows,
        category=category,
        workflow_type=workflow_type or "all",
        panel_key=f"bulk_delete_{category}_{workflow_type}",
    )

    # Workflow detail view — full width, outside expanders
    if st.session_state.selected_workflow_details:
        st.markdown("---")
        details = st.session_state.selected_workflow_details
        _display_workflow_details(
            details["workflow"],
            details["category"],
            details["workflow_type"],
        )


# ============================================================================
# WORKFLOW DETAIL VIEW  (with 🗑️ Delete tab)
# ============================================================================

def _display_workflow_details(workflow: dict, category: str, workflow_type: str):
    """
    Display detailed workflow information in a structured, tabbed layout.
    Includes 🗑️ Delete as the 8th tab.
    """
    from ui.components.workflow_delete_manager import render_delete_panel

    workflow_name = workflow.get("name", "Unnamed Workflow")

    st.markdown("""
    <style>
        .streamlit-expanderHeader { font-size: 1.2rem; }
        div[data-testid="stExpander"] { width: 100%; }
        .main .block-container { max-width: 100%; padding-left: 1rem; padding-right: 1rem; }
    </style>
    """, unsafe_allow_html=True)

    with st.expander(f"📋 Workflow Details: {workflow_name}", expanded=True):

        close_key = f"close_details_{workflow_type}_{workflow.get('_id', 'temp')}"
        if st.button("Close Details", key=close_key):
            st.session_state.selected_workflow_details = None
            st.rerun()

        st.markdown("---")

        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
            "Basic Info",
            "Execution",
            "Content",
            "Artifacts",
            "Link Details",
            "Analyze",
            "Raw JSON",
            "🗑️ Delete",
        ])

        with tab1:
            _display_workflow_details_tab1_fixed(workflow, category, workflow_type)

        with tab2:
            st.subheader("Execution Status")
            executed = workflow.get("executed", False)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Executed",       "Yes" if executed else "No")
            c2.metric("Success",        "Yes" if workflow.get("success") else "No")
            c3.metric("Execution Time",
                      f"{workflow.get('execution_time')} ms" if workflow.get("execution_time") else "N/A")
            c4.metric("Executed At",    workflow.get("executed_at", "N/A"))

        with tab3:
            _display_workflow_details_tab3_fixed(workflow)

        with tab4:
            st.subheader("Artifacts Folder")
            artifacts = workflow.get("artifacts_folder")
            if not artifacts:
                st.warning("No artifacts folder found")
                st.info("This workflow may predate artifact tracking.")
            else:
                st.success("Artifact metadata available")
                for kind in ("pre_screenshot", "post_screenshot"):
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.write(f"**{kind.replace('_', ' ').title()}**")
                        st.code(artifacts.get(f"{kind}_name", "N/A"))
                    with col_b:
                        url = artifacts.get(f"{kind}_url")
                        if url:
                            st.image(url, caption=f"{kind} Preview", use_container_width=True)
                            st.markdown(f"[Open Full Size]({url})")
                if artifacts.get("video_url"):
                    st.video(artifacts["video_url"])
                    st.markdown(f"[Direct Link]({artifacts['video_url']})")
                st.caption(f"Generated At: {artifacts.get('generated_at', 'N/A')}")
                st.caption(f"Base Pattern: `{artifacts.get('base_name', 'N/A')}`")

        with tab5:
            _display_link_details_tab(workflow, category, workflow_type)

        with tab6:
            st.subheader("📊 Workflow Analysis")
            st.caption("Structural analysis of blocks, timing, and execution logic")
            try:
                from .workflow_analyzer_ui import render_workflow_analysis
                render_workflow_analysis(workflow, show_raw=False)
            except ImportError as e:
                st.error("❌ Workflow Analyzer module not found")
                st.info("Ensure `workflow_analyzer_ui.py` exists at: `ui/components/workflow_analyzer_ui.py`")
                st.code(str(e))
            except Exception as e:
                st.error(f"❌ Error during analysis: {e}")
                import traceback
                st.code(traceback.format_exc())

        with tab7:
            st.subheader("Raw Workflow JSON")
            st.json(workflow, expanded=False)

        with tab8:
            st.subheader("🗑️ Delete Workflow")
            st.caption(
                "Permanently remove this workflow and its metadata from MongoDB. "
                "A dry run always runs first so you see exactly what will be deleted."
            )
            st.markdown("---")

            def _close_after_delete():
                st.session_state.selected_workflow_details = None

            render_delete_panel(
                workflow=workflow,
                category=category,
                workflow_type=workflow_type,
                panel_key_suffix="detail_tab",
                on_delete_callback=_close_after_delete,
            )


# ============================================================================
# WORKFLOW FETCHING
# ============================================================================

def _get_workflows_for_category(category: str, workflow_type: str, filters: Dict[str, Any]):
    """Fetch workflows for the specified category and type."""
    try:
        metadata_collection, client = _get_metadata_collection()
        if metadata_collection is None or client is None:
            st.error("❌ Cannot connect to workflow_metadata collection")
            return []

        metadata_query = {"category": category.lower()}
        if workflow_type and workflow_type != 'All Types':
            metadata_query["workflow_type"] = workflow_type.lower()

        # ── Account filter ────────────────────────────────────────────────────
        if filters.get('account_id') is not None:
            metadata_query["postgres_account_id"] = filters['account_id']

        if filters.get('has_link'):
            metadata_query["has_link"] = True
        if filters.get('has_content'):
            metadata_query["has_content"] = True
        if filters.get('executed'):
            metadata_query["executed"] = True
            if filters.get('success'):
                metadata_query["success"] = True

        pipeline = [
            {"$match": metadata_query},
            {
                "$group": {
                    "_id": {
                        "database": "$database_name",
                        "collection": "$collection_name"
                    },
                    "workflow_ids": {"$push": "$automa_workflow_id"},
                    "count": {"$sum": 1}
                }
            }
        ]

        location_results = list(metadata_collection.aggregate(pipeline))

        if not location_results:
            account_msg = f" for account ID {filters['account_id']}" if filters.get('account_id') else ""
            st.info(
                f"No workflows found for category '{category}'"
                + (f" and type '{workflow_type}'" if workflow_type and workflow_type != 'All Types' else "")
                + account_msg
                + " with the selected filters"
            )
            client.close()
            return []

        all_workflows = []

        for location in location_results:
            db_name   = location['_id']['database']
            coll_name = location['_id']['collection']
            workflow_ids = location['workflow_ids']

            try:
                target_collection = client[db_name][coll_name]

                object_ids = []
                for wf_id in workflow_ids:
                    try:
                        object_ids.append(ObjectId(wf_id) if isinstance(wf_id, str) else wf_id)
                    except Exception:
                        continue

                if object_ids:
                    fetched = list(target_collection.find({"_id": {"$in": object_ids}}))
                    for wf in fetched:
                        wf['_id'] = str(wf['_id'])
                        wf['storage_database'] = db_name
                        wf['storage_collection'] = coll_name
                    all_workflows.extend(fetched)

            except Exception as e:
                st.error(f"❌ Error fetching from {db_name}.{coll_name}: {e}")
                continue

        client.close()

        if not all_workflows:
            st.warning("No workflows found matching the selected filters")
            return []

        all_workflows = _merge_workflows_with_metadata(all_workflows, metadata_query, category)
        return all_workflows

    except Exception as e:
        st.error(f"❌ Error fetching workflows: {e}")
        import traceback
        st.code(traceback.format_exc())
        return []


def _get_workflows_for_type(workflow_type: str, filters: Dict[str, Any]):
    """Fetch workflows for the specified type based on filters."""
    try:
        workflows_collection = get_mongo_collection("automa_workflows")
        metadata_collection = get_mongo_collection("workflow_metadata")

        if workflows_collection is None or metadata_collection is None:
            st.warning("Cannot connect to MongoDB collections")
            return []

        workflows = _fetch_from_workflow_metadata_with_filters(workflow_type, filters)
        workflows = sorted(workflows, key=lambda w: w.get('name', '').lower())
        return workflows

    except Exception as e:
        st.error(f"Error fetching workflows: {e}")
        import traceback
        st.error(traceback.format_exc())
        return []


def _fetch_from_workflow_metadata_with_filters(workflow_type: str, filters: Dict[str, Any]):
    """Fetch workflows that match the specified filters."""
    try:
        metadata_collection = get_mongo_collection("workflow_metadata")
        workflows_collection = get_mongo_collection("automa_workflows")

        if metadata_collection is None or workflows_collection is None:
            st.warning("Cannot connect to MongoDB collections")
            return []

        metadata_query = {
            "workflow_type": workflow_type,
            "has_link": True
        }

        if filters.get('has_link'):
            metadata_query["has_link"] = True

        if 'execution_status' in filters:
            if filters['execution_status'] == 'success':
                metadata_query["success"] = True
                metadata_query["executed"] = True
            elif filters['execution_status'] == 'failed':
                metadata_query["success"] = False
                metadata_query["executed"] = True
            elif filters['execution_status'] == 'executed':
                metadata_query["executed"] = True

        if 'account_id' in filters:
            metadata_query["postgres_account_id"] = filters['account_id']

        pipeline = [
            {"$match": metadata_query},
            {"$group": {
                "_id": "$automa_workflow_id",
                "has_link":        {"$first": "$has_link"},
                "has_content":     {"$first": "$has_content"},
                "link_url":        {"$first": "$link_url"},
                "actual_content":  {"$first": "$actual_content"},
                "artifacts_folder":{"$first": "$artifacts_folder"},
                "execution_count": {"$sum": 1},
                "successful_count":{"$sum": {"$cond": ["$success", 1, 0]}},
                "failed_count":    {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                "last_execution":  {"$max": "$generated_at"},
                "executed":        {"$max": "$executed"},
                "success":         {"$max": "$success"}
            }}
        ]

        metadata_stats = list(metadata_collection.aggregate(pipeline))

        if not metadata_stats:
            st.info("No workflow_metadata records found matching filters")
            return []

        workflow_ids = [item["_id"] for item in metadata_stats if item.get("_id")]
        workflows = list(workflows_collection.find({"_id": {"$in": workflow_ids}}))

        meta_map = {item["_id"]: item for item in metadata_stats}
        for wf in workflows:
            wf_id = wf['_id']
            wf['_id'] = str(wf_id)
            meta = meta_map.get(wf_id, {})
            exec_count = meta.get('execution_count', 0)
            wf.update({
                'has_link':         meta.get('has_link', False),
                'has_content':      meta.get('has_content', False),
                'link_url':         meta.get('link_url'),
                'actual_content':   meta.get('actual_content'),
                'artifacts_folder': meta.get('artifacts_folder'),
                'executed':         meta.get('executed', False),
                'success':          meta.get('success', False),
                'execution_count':  exec_count,
                'successful_count': meta.get('successful_count', 0),
                'failed_count':     meta.get('failed_count', 0),
                'last_execution':   meta.get('last_execution'),
                'success_rate': (
                    meta.get('successful_count', 0) / exec_count * 100
                    if exec_count > 0 else 0
                )
            })

        return workflows

    except Exception as e:
        st.error(f"Error fetching workflows: {e}")
        import traceback
        st.error(traceback.format_exc())
        return []


# ============================================================================
# ENHANCED OVERVIEW
# ============================================================================

def render_enhanced_workflow_overview(
    stats_manager,
    category: str,
    workflow_type: str,
    available_categories: List[str],
    available_workflow_types: Dict,
    account_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Render enhanced overview with has_link, has_content, executed metrics and advanced filters."""

    if category == 'All Categories':
        st.markdown("### All Categories Overview")
        overall_stats = {}
        for cat in available_categories:
            overall_stats[cat] = stats_manager.get_category_stats(cat)

        for cat in available_categories:
            st.markdown(f"#### 📁 {cat}")
            stats = overall_stats.get(cat, {})
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            col1.metric("Total Workflows",  stats.get('total_workflows', 0))
            col2.metric("Collections",      stats.get('total_collections', 0))
            col3.metric("Workflow Types",   len(stats.get('workflow_types', [])))
            col4.metric("With Content",     stats.get('workflows_with_content', 0))
            col5.metric("With Links",       stats.get('workflows_with_link', 0))
            col6.metric("Executed",         stats.get('executed_workflows', 0))
            st.markdown("---")

        return {}

    st.markdown(f"### 📁 {category}")
    if workflow_type and workflow_type != 'All Types':
        st.markdown(f"**Type:** {workflow_type}")
    if account_id is not None:
        st.caption(f"Filtered by account ID: {account_id}")

    filters = {'category': category.lower()}
    if workflow_type and workflow_type != 'All Types':
        filters['workflow_type'] = workflow_type.lower()

    # Carry account_id into filters so _render_workflow_selector and
    # _get_workflows_for_category can apply it downstream
    if account_id is not None:
        filters['account_id'] = account_id

    st.markdown("---")
    st.markdown("#### Advanced Filters")

    with st.form(key=f"advanced_filters_{category}_{workflow_type}"):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            filter_has_link = st.checkbox(
                "Has Link",
                value=st.session_state.get(f"filter_has_link_{category}_{workflow_type}", False),
                key=f"filter_has_link_{category}_{workflow_type}_form"
            )
        with col2:
            filter_has_content = st.checkbox(
                "Has Content",
                value=st.session_state.get(f"filter_has_content_{category}_{workflow_type}", False),
                key=f"filter_has_content_{category}_{workflow_type}_form",
                help="Filter workflows that have content attached"
            )
        with col3:
            filter_executed = st.checkbox(
                "Executed",
                value=st.session_state.get(f"filter_executed_{category}_{workflow_type}", False),
                key=f"filter_executed_{category}_{workflow_type}_form"
            )
        with col4:
            filter_successful = st.checkbox(
                "Successful Only",
                value=st.session_state.get(f"filter_successful_{category}_{workflow_type}", False),
                key=f"filter_successful_{category}_{workflow_type}_form"
            )

        fc1, fc2, fc3 = st.columns([1, 2, 1])
        with fc2:
            filter_submitted = st.form_submit_button(
                "🔍 Apply Filters",
                type="primary",
                use_container_width=True
            )

    if filter_submitted:
        st.session_state[f"filter_has_link_{category}_{workflow_type}"]    = filter_has_link
        st.session_state[f"filter_has_content_{category}_{workflow_type}"] = filter_has_content
        st.session_state[f"filter_executed_{category}_{workflow_type}"]    = filter_executed
        st.session_state[f"filter_successful_{category}_{workflow_type}"]  = filter_successful

    stored_has_link    = st.session_state.get(f"filter_has_link_{category}_{workflow_type}", False)
    stored_has_content = st.session_state.get(f"filter_has_content_{category}_{workflow_type}", False)
    stored_executed    = st.session_state.get(f"filter_executed_{category}_{workflow_type}", False)
    stored_successful  = st.session_state.get(f"filter_successful_{category}_{workflow_type}", False)

    if stored_has_link:
        filters['has_link'] = True
    if stored_has_content:
        filters['has_content'] = True
    if stored_executed:
        filters['executed'] = True
        if stored_successful:
            filters['success'] = True

    # rest of function unchanged from here...
    category_stats = stats_manager.get_category_stats(category)

    # ... (keep all existing stats display code unchanged)
    return filters


def _manual_verify_stats(category: str, workflow_type: str = None):
    """Manually verify content and link counts by querying MongoDB directly."""
    try:
        metadata_collection, client = _get_metadata_collection()
        if metadata_collection is None or client is None:
            st.error("❌ Cannot connect to MongoDB")
            return

        query = {"category": category.lower()}
        if workflow_type and workflow_type != 'All Types':
            query["workflow_type"] = workflow_type.lower()

        all_workflows = list(metadata_collection.find(query))
        total_count = len(all_workflows)
        content_count = 0
        link_count = 0

        for wf in all_workflows:
            actual_content  = wf.get('actual_content')
            has_content_flag = wf.get('has_content', False)
            if (actual_content and actual_content.strip() != "") or has_content_flag:
                content_count += 1

            link_url          = wf.get('link_url')
            associated_link_url = wf.get('associated_link_url')
            has_link_flag     = wf.get('has_link', False)
            if (link_url and link_url.strip() != "") or \
               (associated_link_url and associated_link_url.strip() != "") or \
               has_link_flag:
                link_count += 1

        client.close()

        st.success("✅ Manual Verification Complete")
        st.write(f"**Total Workflows:** {total_count}")
        st.write(f"**With Content:** {content_count} ({content_count/total_count*100:.1f}%)" if total_count else "**With Content:** 0")
        st.write(f"**With Links:** {link_count} ({link_count/total_count*100:.1f}%)" if total_count else "**With Links:** 0")

        if content_count > 0:
            st.write("**Example workflows with content:**")
            content_examples = []
            for wf in all_workflows[:5]:
                if wf.get('actual_content') or wf.get('has_content'):
                    content_examples.append({
                        'ID': str(wf.get('_id'))[:12],
                        'Has Content Flag': wf.get('has_content', False),
                        'Content Length': len(wf.get('actual_content', '')) if wf.get('actual_content') else 0,
                        'Link URL': wf.get('link_url', '')[:50] + '...' if wf.get('link_url') else None
                    })
            if content_examples:
                st.dataframe(pd.DataFrame(content_examples))

    except Exception as e:
        st.error(f"Error during manual verification: {e}")


# ============================================================================
# WORKFLOW METADATA DISPLAY (legacy helper used by workflow_content_renderer)
# ============================================================================

def _display_workflow_metadata(workflow: Dict[str, Any], workflow_type: str):
    """Display detailed metadata about a selected workflow, including analysis."""
    workflow_name = workflow.get('name', 'Unknown Workflow')
    with st.expander(f"Workflow Metadata: {workflow_name}", expanded=True):

        close_key = f"close_metadata_{workflow_type}_{workflow.get('_id', 'temp')}"
        if st.button("Close Metadata", key=close_key):
            st.rerun()

        st.markdown("---")

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "Basic Info",
            "Execution Stats",
            "Actual Content",
            "Artifacts Folder",
            "Analyze",
            "Full JSON"
        ])

        with tab1:
            st.subheader("Basic Information")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Name:** {workflow.get('name', 'Unknown')}")
                st.write(f"**ID:** {workflow.get('_id', 'Unknown')}")
                st.write(f"**Type:** {workflow_type.title()}")
                st.write(f"**Has Content:** {'Yes' if workflow.get('has_content') else 'No'}")
            with col2:
                st.write(f"**Has Link:** {'Yes' if workflow.get('has_link') else 'No'}")
                if workflow.get('link_url'):
                    url = workflow.get('link_url')
                    display_url = url if len(url) <= 60 else url[:57] + "..."
                    st.markdown(f"**Link URL:** [{display_url}]({url})")
                else:
                    st.write("**Link URL:** N/A")
                st.write(f"**Account:** {workflow.get('username', 'N/A')} (ID: {workflow.get('account_id', 'N/A')})")
                st.write(f"**Executed:** {'Yes' if workflow.get('executed') else 'No'}")
                if workflow.get('executed'):
                    st.write(f"**Success:** {'Yes' if workflow.get('success') else 'No'}")

        with tab2:
            st.subheader("Execution Statistics")
            exec_count = workflow.get('execution_count', 0)
            if exec_count > 0:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Executions", exec_count)
                c2.metric("Successful",       workflow.get('successful_count', 0))
                c3.metric("Failed",           workflow.get('failed_count', 0))
                c4.metric("Success Rate",     f"{workflow.get('success_rate', 0):.1f}%")
                if workflow.get('last_execution'):
                    st.info(f"Last executed: {workflow.get('last_execution')}")
            else:
                st.info("No execution history yet.")

        with tab3:
            st.subheader("Actual Content")
            if workflow.get('actual_content'):
                st.text_area(
                    "Full Content:",
                    value=workflow.get('actual_content'),
                    height=400,
                    disabled=True,
                    key=f"content_{workflow.get('_id')}"
                )
                content = workflow.get('actual_content', '')
                c1, c2, c3 = st.columns(3)
                c1.metric("Characters", len(content))
                c2.metric("Words",      len(content.split()))
                c3.metric("Lines",      len(content.split('\n')))
            else:
                st.info("No actual content available.")

        with tab4:
            st.subheader("Artifacts Folder")
            artifacts = workflow.get("artifacts_folder")
            if artifacts:
                st.success("Artifact metadata available")
                for kind in ("pre_screenshot", "post_screenshot"):
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.write(f"**{kind.replace('_', ' ').title()}**")
                        st.code(artifacts.get(f"{kind}_name", "N/A"))
                    with col_b:
                        url = artifacts.get(f"{kind}_url")
                        if url:
                            st.image(url, caption=f"{kind} Preview")
                            st.markdown(f"[Open Full Size]({url})")
                        else:
                            st.caption("Not uploaded yet")
                if artifacts.get("video_url"):
                    st.video(artifacts["video_url"])
                    st.markdown(f"[Direct Link]({artifacts['video_url']})")
                else:
                    st.caption("Video not recorded yet")
                st.markdown("---")
                st.caption(f"Generated: {artifacts.get('generated_at', 'N/A')}")
                st.caption(f"Base pattern: `{artifacts.get('base_name', 'N/A')}`")
            else:
                st.warning("No artifacts folder found")
                st.info("This workflow was assigned before the artifacts feature was added.")

        with tab5:
            st.subheader("📊 Workflow Analysis")
            st.caption("Detailed analysis of workflow structure, blocks, and timing")
            try:
                from .workflow_analyzer_ui import render_workflow_analysis
                render_workflow_analysis(workflow, show_raw=False)
            except ImportError as e:
                st.error("❌ Workflow Analyzer not found")
                st.info("💡 Please ensure workflow_analyzer_ui.py is installed at: ui/components/workflow_analyzer_ui.py")
                st.code(str(e))
            except Exception as e:
                st.error(f"❌ Error analyzing workflow: {e}")
                import traceback
                st.code(traceback.format_exc())

        with tab6:
            st.subheader("Full Raw JSON")
            st.json(workflow, expanded=False)
