import psycopg2
import pandas as pd
import os
from datetime import datetime

# Database connection parameters from Docker Compose
DB_PARAMS = {
    'dbname': 'messages',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': '5432'
}

# Updated list of tables and views to export based on new schema
TABLES_AND_VIEWS = [
    'accounts',
    'account_cookies',  # NEW: Cookie management table
    'prompts',
    'workflows',
    'replies',
    'messages',
    'retweets',
    'links',
    'workflow_sync_log',
    'workflow_generation_log',
    'workflow_runs',
    'account_workflow_summary',  # View (updated with cookie fields)
    'workflow_processing_status',  # View
    'account_cookie_summary'  # NEW: Cookie summary view
]

# Output directory for CSV files
OUTPUT_DIR = './data'
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

def export_table_to_csv(table_name: str, conn):
    """Export a single table or view to a CSV file."""
    try:
        # Query to fetch all data from the table/view
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Define CSV file path with timestamp
        csv_file = os.path.join(OUTPUT_DIR, f"{table_name}_{TIMESTAMP}.csv")
        
        # Save to CSV
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"Successfully exported {table_name} to {csv_file} ({len(df)} rows)")
        
        # Special handling for account_cookies to show cookie count
        if table_name == 'account_cookies' and not df.empty:
            total_cookies = df['cookie_count'].sum()
            active_cookies = len(df[df['is_active'] == True])
            print(f"  → Total cookies in storage: {total_cookies}")
            print(f"  → Active cookie sets: {active_cookies}")
        
    except Exception as e:
        print(f"Error exporting {table_name}: {str(e)}")

def print_export_summary(conn):
    """Print a summary of the database state before export."""
    try:
        cursor = conn.cursor()
        
        # Get account statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_accounts,
                SUM(CASE WHEN has_cookies THEN 1 ELSE 0 END) as accounts_with_cookies,
                SUM(CASE WHEN profile_type = 'local_chrome' THEN 1 ELSE 0 END) as local_chrome,
                SUM(CASE WHEN profile_type = 'hyperbrowser' THEN 1 ELSE 0 END) as hyperbrowser
            FROM accounts
        """)
        stats = cursor.fetchone()
        
        # Get active cookie count
        cursor.execute("SELECT COUNT(*) FROM account_cookies WHERE is_active = TRUE")
        active_cookies = cursor.fetchone()[0]
        
        print("\n" + "="*50)
        print("DATABASE EXPORT SUMMARY")
        print("="*50)
        print(f"Total Accounts: {stats[0]}")
        print(f"Accounts with Cookies: {stats[1]}")
        print(f"Active Cookie Sets: {active_cookies}")
        print(f"Local Chrome Profiles: {stats[2]}")
        print(f"Hyperbrowser Profiles: {stats[3]}")
        print(f"Export Timestamp: {TIMESTAMP}")
        print("="*50 + "\n")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error generating summary: {str(e)}")

def main():
    """Connect to PostgreSQL and export all tables and views to CSV."""
    try:
        # Establish database connection
        conn = psycopg2.connect(**DB_PARAMS)
        print("Connected to PostgreSQL database")
        
        # Print summary before export
        print_export_summary(conn)
        
        # Export each table/view
        print("Starting export...\n")
        for table_name in TABLES_AND_VIEWS:
            export_table_to_csv(table_name, conn)
        
        print("\n" + "="*50)
        print("Export completed successfully!")
        print(f"All files saved to: {OUTPUT_DIR}")
        print("="*50)
        
        # Close connection
        conn.close()
        print("\nDatabase connection closed")
        
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")

if __name__ == "__main__":
    main()