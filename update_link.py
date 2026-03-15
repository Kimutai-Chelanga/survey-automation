#!/usr/bin/env python3
"""
update_link.py — DAG-Compatible Test Data Loader
- Creates links that match the EXACT query conditions
- Query expects: tweeted_time IS NULL, tweet_id IS NOT NULL, tweet_id numeric, used = FALSE
"""

import psycopg2
import pandas as pd
import re
from datetime import datetime, timedelta
from random import randint, choice
import secrets

# ===============================
# CONFIG
# ===============================
DB_PARAMS = {
    "dbname": "messages",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432",
}
CSV_FILE = "./link.csv"

# DAG-compatible settings
DAG_COMPATIBLE_MODE = True  # Set to True for DAG processing


# ===============================
# Helpers
# ===============================
def connect_postgres():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        print("Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Error connecting: {e}")
        return None


def load_links_from_csv(csv_file):
    df = pd.read_csv(csv_file)
    df.columns = df.columns.str.strip()
    if "link" not in df.columns:
        raise ValueError("CSV must have 'link' column")
    df["link"] = df["link"].astype(str).str.strip()
    return df


def extract_tweet_id(link):
    """Extract tweet ID from X/Twitter link"""
    m = re.search(r"status/(\d+)", link)
    return m.group(1) if m else "0"


def get_valid_account_ids(cursor):
    """Fetch all real account_id values from accounts table."""
    cursor.execute("SELECT account_id FROM accounts;")
    rows = cursor.fetchall()
    account_ids = [row[0] for row in rows]
    if not account_ids:
        raise ValueError("No accounts found! Insert at least one account first.")
    print(f"Found {len(account_ids)} valid account(s): {account_ids}")
    return account_ids


def dummy_mongo_id():
    return secrets.token_hex(12)


def dummy_timestamp(days_ago=0):
    return datetime.now() - timedelta(days=days_ago)


def get_dag_compatible_values():
    return {
        "tweeted_time": None,
        "used": False,
        "processed_by_workflow": False,

        # Updated values:
        "filtered": True,
        "filtered_time": dummy_timestamp(days_ago=0),
        "within_limit": True,

        "used_time": None,
        "workflow_id": None,
        "mongo_workflow_id": None,
        "workflow_processed_time": None,
        "workflow_status": None,
        "tweeted_date": None,

        "scraped_time": dummy_timestamp(days_ago=randint(0, 1)),
        "mongo_object_id": dummy_mongo_id(),
    }



def get_test_values():
    """
    Returns dummy test values for non-DAG testing.
    Can have any values for testing database structure.
    """
    filtered = choice([True, False])
    processed = choice([True, False])
    
    return {
        "processed_by_workflow": processed,
        "used": choice([True, False]),
        "filtered": filtered,
        "used_time": dummy_timestamp(days_ago=randint(0, 5)) if choice([True, False]) else None,
        "filtered_time": dummy_timestamp(days_ago=randint(0, 3)) if filtered else None,
        "workflow_id": None,
        "mongo_workflow_id": dummy_mongo_id() if choice([True, False]) else None,
        "workflow_processed_time": dummy_timestamp(days_ago=randint(0, 2)) if processed else None,
        "workflow_status": choice(["pending", "completed", "failed", "no_workflows_available"]),
        "tweeted_time": dummy_timestamp(days_ago=randint(0, 30)),
        "within_limit": choice([True, False]),
        "scraped_time": dummy_timestamp(days_ago=randint(0, 7)),
        "mongo_object_id": dummy_mongo_id(),
    }


def update_or_insert_links(conn, df):
    with conn.cursor() as cur:
        # Load valid foreign key values
        valid_account_ids = get_valid_account_ids(cur)

        updated = 0
        inserted = 0
        skipped = 0

        for _, row in df.iterrows():
            link = row["link"]
            tweet_id = extract_tweet_id(link)

            # Validate tweet_id - MUST BE NUMERIC STRING
            if not tweet_id or tweet_id == "0" or not tweet_id.isdigit():
                print(f"⚠️  Skipping link (invalid tweet_id '{tweet_id}'): {link}")
                skipped += 1
                continue

            # Verify tweet_id is numeric (matches regex ^[0-9]+$)
            if not re.match(r'^[0-9]+$', tweet_id):
                print(f"⚠️  Skipping link (tweet_id not numeric): {link}")
                skipped += 1
                continue

            # Pick real account_id (default to 1 if available)
            if 1 in valid_account_ids:
                account_id = 1
            else:
                account_id = choice(valid_account_ids)

            # Get appropriate values based on mode
            if DAG_COMPATIBLE_MODE:
                values = get_dag_compatible_values()
                mode_indicator = "🎯"
            else:
                values = get_test_values()
                mode_indicator = "🧪"

            # Calculate tweeted_date from tweeted_time
            tweeted_date = values["tweeted_time"].date() if values["tweeted_time"] else None

            # Check if link already exists and is processed
            cur.execute(
                "SELECT links_id, processed_by_workflow, used, tweeted_time FROM links WHERE link = %s",
                (link,)
            )
            existing = cur.fetchone()

            if existing:
                existing_id, existing_processed, existing_used, existing_tweeted_time = existing
                
                # Skip if already processed (unless in test mode)
                if DAG_COMPATIBLE_MODE and existing_processed:
                    print(f"⏭️  Skipping (already processed): {link} (id={existing_id})")
                    skipped += 1
                    continue

            # Prepare full data dict
            data = {
                "account_id": account_id,
                "link": link,
                "tweet_id": tweet_id,  # Guaranteed to be numeric string
                "tweeted_time": values["tweeted_time"],
                "tweeted_date": tweeted_date,
                "within_limit": values["within_limit"],
                "scraped_time": values["scraped_time"],
                "used": values["used"],
                "used_time": values["used_time"],
                "filtered": values["filtered"],
                "filtered_time": values["filtered_time"],
                "mongo_object_id": values["mongo_object_id"],
                "workflow_id": values["workflow_id"],
                "mongo_workflow_id": values["mongo_workflow_id"],
                "processed_by_workflow": values["processed_by_workflow"],
                "workflow_processed_time": values["workflow_processed_time"],
                "workflow_status": values["workflow_status"],
            }

            # Try UPDATE first
            update_sql = """
                UPDATE links SET
                    account_id = %(account_id)s,
                    tweet_id = %(tweet_id)s,
                    tweeted_time = %(tweeted_time)s,
                    tweeted_date = %(tweeted_date)s,
                    within_limit = %(within_limit)s,
                    scraped_time = %(scraped_time)s,
                    used = %(used)s,
                    used_time = %(used_time)s,
                    filtered = %(filtered)s,
                    filtered_time = %(filtered_time)s,
                    mongo_object_id = %(mongo_object_id)s,
                    workflow_id = %(workflow_id)s,
                    mongo_workflow_id = %(mongo_workflow_id)s,
                    processed_by_workflow = %(processed_by_workflow)s,
                    workflow_processed_time = %(workflow_processed_time)s,
                    workflow_status = %(workflow_status)s
                WHERE link = %(link)s
                RETURNING links_id;
            """
            cur.execute(update_sql, data)
            result = cur.fetchone()

            if result:
                links_id = result[0]
                print(f"{mode_indicator} Updated link (id={links_id}): {link}")
                if DAG_COMPATIBLE_MODE:
                    print(f"   ✓ tweeted_time=NULL, used=FALSE, tweet_id={tweet_id} (numeric)")
                updated += 1
            else:
                # INSERT new link
                insert_sql = """
                    INSERT INTO links (
                        account_id, link, tweet_id, tweeted_time, tweeted_date,
                        within_limit, scraped_time, used, used_time,
                        filtered, filtered_time, mongo_object_id,
                        workflow_id, mongo_workflow_id,
                        processed_by_workflow, workflow_processed_time,
                        workflow_status
                    ) VALUES (
                        %(account_id)s, %(link)s, %(tweet_id)s,
                        %(tweeted_time)s, %(tweeted_date)s,
                        %(within_limit)s, %(scraped_time)s,
                        %(used)s, %(used_time)s,
                        %(filtered)s, %(filtered_time)s,
                        %(mongo_object_id)s,
                        %(workflow_id)s, %(mongo_workflow_id)s,
                        %(processed_by_workflow)s, %(workflow_processed_time)s,
                        %(workflow_status)s
                    ) RETURNING links_id;
                """
                cur.execute(insert_sql, data)
                links_id = cur.fetchone()[0]
                print(f"{mode_indicator} Inserted new link (id={links_id}): {link}")
                if DAG_COMPATIBLE_MODE:
                    print(f"   ✓ Matches ALL query conditions")
                inserted += 1

        print(f"\n{'='*60}")
        print(f"Summary: {updated} updated, {inserted} inserted, {skipped} skipped")
        if DAG_COMPATIBLE_MODE:
            print(f"\n✅ Links match EXACT query conditions:")
            print(f"   ✓ tweeted_time IS NULL")
            print(f"   ✓ tweet_id IS NOT NULL AND tweet_id != ''")
            print(f"   ✓ tweet_id ~ '^[0-9]+$' (numeric)")
            print(f"   ✓ used = FALSE")
            print(f"   ✓ processed_by_workflow = FALSE")
        print(f"{'='*60}")


# ===============================
# Main
# ===============================
def main():
    mode_name = "DAG-COMPATIBLE" if DAG_COMPATIBLE_MODE else "TEST"
    print(f"\n{'='*60}")
    print(f"Updating links in {mode_name} mode...")
    print(f"{'='*60}\n")
    
    if DAG_COMPATIBLE_MODE:
        print("🎯 DAG-Compatible Mode - EXACT QUERY MATCH:")
        print("   ✓ tweeted_time = NULL (not set)")
        print("   ✓ tweet_id = numeric string (validated)")
        print("   ✓ used = FALSE")
        print("   ✓ processed_by_workflow = FALSE")
        print("   → Links will be picked up by DAG query\n")
    else:
        print("🧪 Test Mode:")
        print("   - Random values for testing")
        print("   - Links may not match query conditions\n")
    
    conn = connect_postgres()
    if not conn:
        return

    try:
        df = load_links_from_csv(CSV_FILE)
        print(f"Loaded {len(df)} links from {CSV_FILE}\n")
        update_or_insert_links(conn, df)
        print("\n✅ All links ready for DAG processing.")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()