import logging
from psycopg2 import Error as Psycopg2Error
import streamlit as st
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)

# ===================================================================
# POSTGRESQL SCHEMA - FULLY ALIGNED WITH init-db.sql
# Updated: 2026-03-06
# Changes from previous version:
#   1. Added account_extraction_state table
#   2. Added success/failure columns to links table
#   3. Added tweet_author_user_id and chat_link columns to links table
#   4. Added extraction state helper functions (reset_extraction_state,
#      reset_all_extraction_state, upsert_extraction_state)
#   5. Added build_chat_link helper function
#   6. Added extraction state trigger function + trigger
#   7. Added links_with_chat view
#   8. Updated parent_tweets_in_links view to expose new columns
#   9. Added extraction_state_summary view
#  10. Added new indexes for success, failure, tweet_author_user_id, chat_link
# ===================================================================

def create_postgres_tables():
    """
    Creates COMPLETE PostgreSQL schema matching init-db.sql EXACTLY.
    ONLY creates tables if they don't exist - NEVER drops existing data.
    """

    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:

                # ======================================================
                # STEP 1: CREATE CORE TABLES (ONLY IF NOT EXISTS)
                # ======================================================
                logger.info("Creating COMPLETE PostgreSQL schema...")

                # ACCOUNTS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        account_id SERIAL PRIMARY KEY,
                        username VARCHAR(255) NOT NULL UNIQUE,
                        x_account_id VARCHAR(30),          
                        profile_id VARCHAR(255) UNIQUE,
                        profile_type VARCHAR(50) DEFAULT 'hyperbrowser'
                            CHECK (profile_type IN ('local_chrome', 'hyperbrowser')),
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        mongo_object_id VARCHAR(24),
                        total_content_processed INTEGER DEFAULT 0,
                        has_cookies BOOLEAN DEFAULT FALSE,
                        cookies_last_updated TIMESTAMP
                    )
                """)
                logger.info("✓ accounts table ready")

                # ACCOUNT COOKIES TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS account_cookies (
                        cookie_id SERIAL PRIMARY KEY,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        cookie_data JSONB NOT NULL,
                        cookie_count INTEGER NOT NULL DEFAULT 0,
                        uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        cookie_source VARCHAR(100) DEFAULT 'editthiscookie',
                        notes TEXT,
                        CONSTRAINT unique_active_cookie_per_account UNIQUE (account_id, is_active)
                    )
                """)
                logger.info("✓ account_cookies table ready")

                # ACCOUNT EXTRACTION STATE TABLE
                # Tracks the newest tweet ID seen per account for incremental extraction.
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS account_extraction_state (
                        state_id               SERIAL PRIMARY KEY,
                        username               VARCHAR(255) NOT NULL UNIQUE,
                        last_seen_tweet_id     VARCHAR(30),
                        last_extraction_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_tweet_url         TEXT,
                        tweets_found_last_run  INTEGER DEFAULT 0,
                        parents_found_last_run INTEGER DEFAULT 0,
                        created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("✓ account_extraction_state table ready")

                # WORKFLOWS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS workflows (
                        workflow_id SERIAL PRIMARY KEY,
                        workflow_name VARCHAR(255) NOT NULL,
                        workflow_type VARCHAR(50),
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                logger.info("✓ workflows table ready")

                # PROMPTS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS prompts (
                        prompt_id SERIAL PRIMARY KEY,
                        account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                        name VARCHAR(255) NOT NULL,
                        content TEXT NOT NULL,
                        prompt_type VARCHAR(50) NOT NULL,
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        mongo_object_id VARCHAR(24),
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                logger.info("✓ prompts table ready")

                # PROMPT BACKUPS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS prompt_backups (
                        backup_id SERIAL PRIMARY KEY,
                        prompt_id INTEGER NOT NULL REFERENCES prompts(prompt_id) ON DELETE CASCADE,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        username VARCHAR(255) NOT NULL,
                        prompt_name VARCHAR(255) NOT NULL,
                        prompt_type VARCHAR(50) NOT NULL,
                        prompt_content TEXT NOT NULL,
                        version_number INTEGER NOT NULL DEFAULT 1,
                        backed_up_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        backup_type VARCHAR(50) DEFAULT 'manual'
                            CHECK (backup_type IN ('manual', 'auto', 'pre_update', 'pre_delete')),
                        backup_reason TEXT,
                        metadata JSONB,
                        is_restorable BOOLEAN DEFAULT TRUE,
                        restored BOOLEAN DEFAULT FALSE,
                        restored_at TIMESTAMP,
                        restored_to_prompt_id INTEGER REFERENCES prompts(prompt_id) ON DELETE SET NULL
                    )
                """)
                logger.info("✓ prompt_backups table ready")

                # PROMPT VARIATIONS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS prompt_variations (
                        variation_id SERIAL PRIMARY KEY,
                        parent_prompt_id INTEGER NOT NULL REFERENCES prompts(prompt_id) ON DELETE CASCADE,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        username VARCHAR(255) NOT NULL,
                        prompt_type VARCHAR(50) NOT NULL,
                        prompt_name VARCHAR(255) NOT NULL,
                        variation_content TEXT NOT NULL,
                        variation_number INTEGER NOT NULL,
                        generation_batch_id VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        used BOOLEAN DEFAULT FALSE,
                        used_at TIMESTAMP,
                        copied_count INTEGER DEFAULT 0,
                        last_copied_at TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        quality_score DECIMAL(3,2),
                        metadata JSONB
                    )
                """)
                logger.info("✓ prompt_variations table ready")

                # EXTRACTED URLS TABLE (CHILD TWEETS - REPLIES)
                # NOTE: Created BEFORE links table so links can reference it
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS extracted_urls (
                        extracted_url_id SERIAL PRIMARY KEY,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        url TEXT NOT NULL,
                        tweet_id VARCHAR(30),
                        tweet_text TEXT,
                        is_reply BOOLEAN DEFAULT FALSE,
                        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        parent_extracted BOOLEAN DEFAULT FALSE,
                        parent_extraction_attempted BOOLEAN DEFAULT FALSE,
                        parent_extraction_time TIMESTAMP,
                        parent_url_id INTEGER REFERENCES extracted_urls(extracted_url_id) ON DELETE SET NULL,
                        parent_tweet_id VARCHAR(30),
                        parent_tweet_url TEXT,
                        linked_to_links_table BOOLEAN DEFAULT FALSE,
                        links_table_id INTEGER,
                        source_page TEXT,
                        extraction_batch_id VARCHAR(100),
                        metadata JSONB,
                        CONSTRAINT unique_url_per_account UNIQUE (account_id, url)
                    )
                """)
                logger.info("✓ extracted_urls table ready (child tweets)")

                # LINKS TABLE (PARENT TWEETS - ORIGINALS)
                # Includes: success/failure tracking, tweet_author_user_id, chat_link
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS links (
                        links_id SERIAL PRIMARY KEY,
                        account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                        link TEXT NOT NULL UNIQUE,
                        tweet_id VARCHAR(30),
                        tweeted_time TIMESTAMP,
                        tweeted_date DATE,
                        within_limit BOOLEAN DEFAULT FALSE,
                        scraped_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        used BOOLEAN DEFAULT FALSE,
                        executed BOOLEAN DEFAULT FALSE,
                        used_time TIMESTAMP,
                        filtered BOOLEAN DEFAULT FALSE,
                        filtered_time TIMESTAMP,
                        workflow_type VARCHAR(50),
                        mongo_object_id VARCHAR(24),
                        workflow_id INTEGER REFERENCES workflows(workflow_id) ON DELETE SET NULL,
                        mongo_workflow_id VARCHAR(100),
                        processed_by_workflow BOOLEAN DEFAULT FALSE,
                        workflow_processed_time TIMESTAMP,
                        workflow_status VARCHAR(50) DEFAULT 'pending'
                            CHECK (workflow_status IN ('pending', 'processing', 'completed', 'failed', 'error')),
                        execution_mode VARCHAR(20) DEFAULT 'execution'
                            CHECK (execution_mode IN ('manual', 'execution')),

                        -- Success/Failure tracking
                        success BOOLEAN DEFAULT FALSE,
                        failure BOOLEAN DEFAULT FALSE,

                        -- Connection to extracted_urls (source child tweet)
                        extracted_url_id INTEGER REFERENCES extracted_urls(extracted_url_id) ON DELETE SET NULL,
                        is_parent_tweet BOOLEAN DEFAULT FALSE,
                        child_tweet_id INTEGER REFERENCES links(links_id) ON DELETE SET NULL,

                        -- Link-to-Content connection fields
                        connected_content_id INTEGER,
                        connected_via_workflow VARCHAR(255),
                        content_connection_time TIMESTAMP,
                        connection_status VARCHAR(50) DEFAULT 'pending'
                            CHECK (connection_status IN ('pending', 'active', 'broken', 'disconnected')),

                        -- Author identity & direct message
                        tweet_author_user_id VARCHAR(30),
                        chat_link TEXT
                    )
                """)
                logger.info("✓ links table ready (parent tweets, includes success/failure/user_id/chat_link)")

                # ── Migrate existing links table: add new columns if missing ──
                # Safe to run on both new and existing databases.
                migration_columns = [
                    ("success",              "BOOLEAN DEFAULT FALSE"),
                    ("failure",              "BOOLEAN DEFAULT FALSE"),
                    ("tweet_author_user_id", "VARCHAR(30)"),
                    ("chat_link",            "TEXT"),
                ]
                for col_name, col_def in migration_columns:
                    cursor.execute(f"""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns
                                WHERE table_name = 'links' AND column_name = '{col_name}'
                            ) THEN
                                ALTER TABLE links ADD COLUMN {col_name} {col_def};
                            END IF;
                        END$$;
                    """)
                logger.info("✓ links migration complete (success/failure/tweet_author_user_id/chat_link ensured)")

                # CONTENT TABLE
                # CRITICAL: No FK constraint on connected_link_id (intentional)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS content (
                        content_id SERIAL PRIMARY KEY,
                        account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                        prompt_id INTEGER REFERENCES prompts(prompt_id) ON DELETE SET NULL,
                        content TEXT NOT NULL,
                        content_name VARCHAR(255) NOT NULL,
                        content_type VARCHAR(50) NOT NULL,
                        used BOOLEAN DEFAULT FALSE,
                        used_time TIMESTAMP,
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        mongo_object_id VARCHAR(24),
                        workflow_status VARCHAR(50) DEFAULT 'pending'
                            CHECK (workflow_status IN ('pending', 'processing', 'completed', 'failed', 'error')),

                        -- Workflow connection tracking
                        automa_workflow_id VARCHAR(100),
                        workflow_name VARCHAR(255),
                        workflow_generated_time TIMESTAMP,
                        workflow_executed_time TIMESTAMP,
                        workflow_success BOOLEAN DEFAULT FALSE,
                        has_content BOOLEAN DEFAULT FALSE,

                        -- Link connection tracking (no FK - intentional)
                        connected_link_id INTEGER,
                        connected_via_workflow VARCHAR(255),
                        link_connection_time TIMESTAMP,
                        link_connection_status VARCHAR(50) DEFAULT 'pending'
                            CHECK (link_connection_status IN ('pending', 'active', 'broken', 'disconnected')),

                        -- Variation tracking
                        generated_from_variation_id INTEGER REFERENCES prompt_variations(variation_id) ON DELETE SET NULL
                    )
                """)
                logger.info("✓ content table ready (no FK on connected_link_id)")

                # LINK-CONTENT MAPPINGS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS link_content_mappings (
                        mapping_id SERIAL PRIMARY KEY,
                        link_id INTEGER NOT NULL REFERENCES links(links_id) ON DELETE CASCADE,
                        content_id INTEGER NOT NULL REFERENCES content(content_id) ON DELETE CASCADE,
                        workflow_id VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(link_id, content_id)
                    )
                """)
                logger.info("✓ link_content_mappings table ready")

                # LINK-CONTENT CONNECTIONS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS link_content_connections (
                        connection_id SERIAL PRIMARY KEY,
                        link_id INTEGER NOT NULL REFERENCES links(links_id) ON DELETE CASCADE,
                        content_id INTEGER NOT NULL,
                        workflow_name VARCHAR(255),
                        automa_workflow_id VARCHAR(100),
                        workflow_type VARCHAR(100),
                        account_id INTEGER,
                        connection_type VARCHAR(50) DEFAULT 'workflow_based'
                            CHECK (connection_type IN ('workflow_based', 'manual', 'automated')),
                        connection_method VARCHAR(100) DEFAULT 'filter_links_workflow_assignment',
                        connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR(50) DEFAULT 'active'
                            CHECK (status IN ('active', 'inactive', 'broken')),
                        metadata JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("✓ link_content_connections table ready")

                # WORKFLOW GENERATION LOG
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_generation_log (
                        log_id SERIAL PRIMARY KEY,
                        workflow_type VARCHAR(50) NOT NULL,
                        workflow_name VARCHAR(255) NOT NULL,
                        content_id INTEGER NOT NULL,
                        automa_workflow_id VARCHAR(100) NOT NULL,
                        account_id INTEGER,
                        prompt_id INTEGER,
                        workflow_id VARCHAR(100),
                        username VARCHAR(255),
                        profile_id VARCHAR(255),
                        generated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR(50) DEFAULT 'success'
                            CHECK (status IN ('success', 'failed', 'partial')),
                        error_message TEXT
                    )
                """)
                logger.info("✓ workflow_generation_log table ready")

                # WORKFLOW SYNC LOG
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_sync_log (
                        sync_id SERIAL PRIMARY KEY,
                        sync_type VARCHAR(50) NOT NULL
                            CHECK (sync_type IN ('postgres_to_mongo', 'mongo_to_postgres', 'status_update', 'execution_log')),
                        workflow_type VARCHAR(50) NOT NULL,
                        content_id INTEGER,
                        automa_workflow_id VARCHAR(100),
                        workflow_name VARCHAR(255),
                        account_id INTEGER,
                        sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        sync_status VARCHAR(50) DEFAULT 'success'
                            CHECK (sync_status IN ('success', 'failed', 'partial')),
                        error_message TEXT,
                        details JSONB
                    )
                """)
                logger.info("✓ workflow_sync_log table ready")

                # ======================================================
                # STEP 2: CREATE INDEXES
                # ======================================================
                logger.info("Creating indexes...")

                # Accounts
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounts_username ON accounts(username);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounts_profile_id ON accounts(profile_id);")

                # Account cookies
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_cookies_account_id ON account_cookies(account_id);")

                # Account extraction state
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extraction_state_username ON account_extraction_state(username);")

                # Extracted URLs
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_account_id ON extracted_urls(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_url ON extracted_urls(url);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_tweet_id ON extracted_urls(tweet_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_is_reply ON extracted_urls(is_reply);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_parent_extracted ON extracted_urls(parent_extracted);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_parent_extraction_attempted ON extracted_urls(parent_extraction_attempted);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_linked_to_links ON extracted_urls(linked_to_links_table);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extracted_urls_extraction_batch ON extracted_urls(extraction_batch_id);")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_extracted_urls_needs_parent_extraction
                    ON extracted_urls(account_id, is_reply, parent_extraction_attempted)
                    WHERE is_reply = TRUE AND parent_extraction_attempted = FALSE;
                """)

                # Links
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_account_id ON links(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_workflow_status ON links(workflow_status);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_used ON links(used);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_extracted_url_id ON links(extracted_url_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_is_parent_tweet ON links(is_parent_tweet);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_child_tweet_id ON links(child_tweet_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_connected_content ON links(connected_content_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_success ON links(success);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_failure ON links(failure);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_tweet_author_user_id ON links(tweet_author_user_id);")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_links_chat_link
                    ON links(chat_link)
                    WHERE chat_link IS NOT NULL;
                """)

                # Workflows
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_workflows_type ON workflows(workflow_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_workflows_active ON workflows(is_active);")

                # Prompts
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompts_account_id ON prompts(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompts_type ON prompts(prompt_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompts_is_active ON prompts(is_active);")

                # Prompt backups
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompt_backups_prompt_id ON prompt_backups(prompt_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompt_backups_account_id ON prompt_backups(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompt_backups_username ON prompt_backups(username);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompt_backups_prompt_type ON prompt_backups(prompt_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompt_backups_version ON prompt_backups(prompt_id, version_number);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompt_backups_backed_up_at ON prompt_backups(backed_up_at);")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_prompt_backups_restorable
                    ON prompt_backups(is_restorable)
                    WHERE is_restorable = TRUE;
                """)

                # Prompt variations
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_variations_parent_prompt ON prompt_variations(parent_prompt_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_variations_account_id ON prompt_variations(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_variations_username ON prompt_variations(username);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_variations_prompt_type ON prompt_variations(prompt_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_variations_batch_id ON prompt_variations(generation_batch_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_variations_used ON prompt_variations(used);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_variations_created_at ON prompt_variations(created_at);")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_variations_active
                    ON prompt_variations(is_active)
                    WHERE is_active = TRUE;
                """)

                # Content
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_account_id ON content(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_prompt_id ON content(prompt_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_type ON content(content_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_content_used ON content(used);")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_content_variation
                    ON content(generated_from_variation_id)
                    WHERE generated_from_variation_id IS NOT NULL;
                """)

                # Link-content mappings
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_lcm_link_id ON link_content_mappings(link_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_lcm_content_id ON link_content_mappings(content_id);")

                # Link-content connections
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_lcc_link_id ON link_content_connections(link_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_lcc_content_id ON link_content_connections(content_id);")

                logger.info("✓ All indexes ready (~55 indexes)")

                # ======================================================
                # STEP 3: CREATE OR REPLACE TRIGGER FUNCTIONS
                # ======================================================
                logger.info("Creating trigger functions...")

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION update_updated_time_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_time = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION update_extraction_state_updated_at()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION auto_backup_prompt()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        v_version_number INTEGER;
                        v_backup_type VARCHAR(50);
                    BEGIN
                        IF TG_OP = 'DELETE' THEN
                            v_backup_type := 'pre_delete';
                        ELSE
                            v_backup_type := 'pre_update';
                        END IF;

                        SELECT COALESCE(MAX(version_number), 0) + 1
                        INTO v_version_number
                        FROM prompt_backups
                        WHERE prompt_id = OLD.prompt_id;

                        INSERT INTO prompt_backups (
                            prompt_id, account_id, username, prompt_name, prompt_type,
                            prompt_content, version_number, backup_type, backup_reason, metadata
                        )
                        SELECT
                            OLD.prompt_id, OLD.account_id, a.username, OLD.name, OLD.prompt_type,
                            OLD.content, v_version_number, v_backup_type,
                            CASE WHEN TG_OP = 'DELETE' THEN 'Automatic backup before deletion'
                                 ELSE 'Automatic backup before update' END,
                            jsonb_build_object(
                                'is_active', OLD.is_active,
                                'created_time', OLD.created_time,
                                'updated_time', OLD.updated_time,
                                'mongo_object_id', OLD.mongo_object_id
                            )
                        FROM accounts a
                        WHERE a.account_id = OLD.account_id;

                        RETURN OLD;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION update_variation_usage()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF NEW.used = TRUE AND OLD.used = FALSE THEN
                            NEW.used_at = CURRENT_TIMESTAMP;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                logger.info("✓ Trigger functions ready (4 functions)")

                # ======================================================
                # STEP 4: CREATE TRIGGERS
                # ======================================================
                logger.info("Creating triggers...")

                def trigger_exists(trigger_name, table_name):
                    try:
                        cursor.execute("""
                            SELECT 1 FROM pg_trigger t
                            JOIN pg_class c ON t.tgrelid = c.oid
                            WHERE t.tgname = %s AND c.relname = %s
                            LIMIT 1;
                        """, (trigger_name, table_name))
                        return cursor.fetchone() is not None
                    except Exception as e:
                        logger.error(f"Error checking trigger {trigger_name}: {e}")
                        return False

                triggers = [
                    ("update_accounts_updated_time", "accounts", """
                        CREATE TRIGGER update_accounts_updated_time
                        BEFORE UPDATE ON accounts
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_time_column();
                    """),
                    ("update_prompts_updated_time", "prompts", """
                        CREATE TRIGGER update_prompts_updated_time
                        BEFORE UPDATE ON prompts
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_time_column();
                    """),
                    ("trg_extraction_state_updated_at", "account_extraction_state", """
                        CREATE TRIGGER trg_extraction_state_updated_at
                        BEFORE UPDATE ON account_extraction_state
                        FOR EACH ROW
                        EXECUTE FUNCTION update_extraction_state_updated_at();
                    """),
                    ("backup_prompt_before_update", "prompts", """
                        CREATE TRIGGER backup_prompt_before_update
                        BEFORE UPDATE ON prompts
                        FOR EACH ROW
                        WHEN (OLD.content IS DISTINCT FROM NEW.content OR OLD.name IS DISTINCT FROM NEW.name)
                        EXECUTE FUNCTION auto_backup_prompt();
                    """),
                    ("backup_prompt_before_delete", "prompts", """
                        CREATE TRIGGER backup_prompt_before_delete
                        BEFORE DELETE ON prompts
                        FOR EACH ROW
                        EXECUTE FUNCTION auto_backup_prompt();
                    """),
                    ("update_variation_usage_trigger", "prompt_variations", """
                        CREATE TRIGGER update_variation_usage_trigger
                        BEFORE UPDATE ON prompt_variations
                        FOR EACH ROW
                        EXECUTE FUNCTION update_variation_usage();
                    """),
                ]

                created_count = 0
                existing_count = 0
                for trigger_name, table_name, create_sql in triggers:
                    if not trigger_exists(trigger_name, table_name):
                        try:
                            cursor.execute(create_sql)
                            created_count += 1
                            logger.debug(f"Created trigger: {trigger_name} on {table_name}")
                        except Exception as e:
                            logger.error(f"Failed to create trigger {trigger_name}: {e}")
                    else:
                        existing_count += 1
                        logger.debug(f"Trigger already exists: {trigger_name} on {table_name}")

                logger.info(f"✓ Triggers ready — created: {created_count}, existing: {existing_count}")

                # ======================================================
                # STEP 5: CREATE HELPER FUNCTIONS
                # ======================================================
                logger.info("Creating helper functions...")

                # ── Extracted URLs helpers ─────────────────────────────

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION insert_extracted_url(
                        p_account_id INTEGER,
                        p_url TEXT,
                        p_tweet_id VARCHAR(30),
                        p_tweet_text TEXT DEFAULT NULL,
                        p_is_reply BOOLEAN DEFAULT FALSE,
                        p_source_page TEXT DEFAULT NULL,
                        p_extraction_batch_id VARCHAR(100) DEFAULT NULL,
                        p_metadata JSONB DEFAULT NULL
                    )
                    RETURNS INTEGER AS $$
                    DECLARE
                        v_extracted_url_id INTEGER;
                    BEGIN
                        INSERT INTO extracted_urls (
                            account_id, url, tweet_id, tweet_text, is_reply,
                            source_page, extraction_batch_id, metadata
                        )
                        VALUES (
                            p_account_id, p_url, p_tweet_id, p_tweet_text, p_is_reply,
                            p_source_page, p_extraction_batch_id, p_metadata
                        )
                        ON CONFLICT (account_id, url) DO NOTHING
                        RETURNING extracted_url_id INTO v_extracted_url_id;

                        IF v_extracted_url_id IS NULL THEN
                            SELECT extracted_url_id INTO v_extracted_url_id
                            FROM extracted_urls
                            WHERE account_id = p_account_id AND url = p_url;
                        END IF;

                        RETURN v_extracted_url_id;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_urls_needing_parent_extraction(
                        p_account_id INTEGER,
                        p_limit INTEGER DEFAULT 100
                    )
                    RETURNS TABLE (
                        extracted_url_id INTEGER,
                        url TEXT,
                        tweet_id VARCHAR(30),
                        tweet_text TEXT
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            eu.extracted_url_id,
                            eu.url,
                            eu.tweet_id,
                            eu.tweet_text
                        FROM extracted_urls eu
                        WHERE eu.account_id = p_account_id
                          AND eu.is_reply = TRUE
                          AND eu.parent_extraction_attempted = FALSE
                        ORDER BY eu.extracted_at ASC
                        LIMIT p_limit;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION mark_parent_extraction_attempted(
                        p_extracted_url_id INTEGER,
                        p_parent_found BOOLEAN,
                        p_parent_tweet_id VARCHAR(30) DEFAULT NULL,
                        p_parent_tweet_url TEXT DEFAULT NULL,
                        p_parent_url_id INTEGER DEFAULT NULL
                    )
                    RETURNS BOOLEAN AS $$
                    BEGIN
                        UPDATE extracted_urls
                        SET parent_extraction_attempted = TRUE,
                            parent_extraction_time = CURRENT_TIMESTAMP,
                            parent_extracted = p_parent_found,
                            parent_tweet_id = p_parent_tweet_id,
                            parent_tweet_url = p_parent_tweet_url,
                            parent_url_id = p_parent_url_id
                        WHERE extracted_url_id = p_extracted_url_id;
                        RETURN FOUND;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION insert_parent_tweet_to_links(
                        p_parent_url TEXT,
                        p_parent_tweet_id VARCHAR(30),
                        p_child_extracted_url_id INTEGER,
                        p_account_id INTEGER,
                        p_parent_extracted_url_id INTEGER DEFAULT NULL,
                        p_tweeted_time TIMESTAMP DEFAULT NULL,
                        p_tweeted_date DATE DEFAULT NULL
                    )
                    RETURNS INTEGER AS $$
                    DECLARE
                        v_parent_links_id INTEGER;
                    BEGIN
                        INSERT INTO links (
                            account_id, link, tweet_id, tweeted_time, tweeted_date,
                            extracted_url_id, is_parent_tweet,
                            scraped_time, workflow_status, within_limit, used, filtered
                        )
                        VALUES (
                            p_account_id, p_parent_url, p_parent_tweet_id,
                            p_tweeted_time, p_tweeted_date,
                            p_child_extracted_url_id, TRUE,
                            CURRENT_TIMESTAMP, 'pending', FALSE, FALSE, FALSE
                        )
                        ON CONFLICT (link) DO UPDATE SET
                            is_parent_tweet  = TRUE,
                            extracted_url_id = EXCLUDED.extracted_url_id,
                            account_id       = EXCLUDED.account_id
                        RETURNING links_id INTO v_parent_links_id;

                        IF p_parent_extracted_url_id IS NOT NULL THEN
                            UPDATE extracted_urls
                            SET linked_to_links_table = TRUE,
                                links_table_id        = v_parent_links_id
                            WHERE extracted_url_id = p_parent_extracted_url_id;
                        END IF;

                        UPDATE extracted_urls
                        SET links_table_id = v_parent_links_id
                        WHERE extracted_url_id = p_child_extracted_url_id;

                        RETURN v_parent_links_id;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                # ── Extraction state helpers ───────────────────────────

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION reset_extraction_state(p_username VARCHAR)
                    RETURNS BOOLEAN AS $$
                    BEGIN
                        UPDATE account_extraction_state
                        SET last_seen_tweet_id     = NULL,
                            last_extraction_time   = NULL,
                            tweets_found_last_run  = 0,
                            parents_found_last_run = 0
                        WHERE username = p_username;
                        RETURN FOUND;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION reset_all_extraction_state()
                    RETURNS INTEGER AS $$
                    DECLARE
                        v_count INTEGER;
                    BEGIN
                        UPDATE account_extraction_state
                        SET last_seen_tweet_id     = NULL,
                            last_extraction_time   = NULL,
                            tweets_found_last_run  = 0,
                            parents_found_last_run = 0;
                        GET DIAGNOSTICS v_count = ROW_COUNT;
                        RETURN v_count;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION upsert_extraction_state(
                        p_username              VARCHAR,
                        p_last_seen_tweet_id    VARCHAR(30),
                        p_last_tweet_url        TEXT    DEFAULT NULL,
                        p_tweets_found          INTEGER DEFAULT 0,
                        p_parents_found         INTEGER DEFAULT 0
                    )
                    RETURNS VOID AS $$
                    BEGIN
                        INSERT INTO account_extraction_state (
                            username,
                            last_seen_tweet_id,
                            last_extraction_time,
                            last_tweet_url,
                            tweets_found_last_run,
                            parents_found_last_run
                        )
                        VALUES (
                            p_username,
                            p_last_seen_tweet_id,
                            NOW(),
                            p_last_tweet_url,
                            p_tweets_found,
                            p_parents_found
                        )
                        ON CONFLICT (username) DO UPDATE
                            SET last_seen_tweet_id     = EXCLUDED.last_seen_tweet_id,
                                last_extraction_time   = EXCLUDED.last_extraction_time,
                                last_tweet_url         = EXCLUDED.last_tweet_url,
                                tweets_found_last_run  = EXCLUDED.tweets_found_last_run,
                                parents_found_last_run = EXCLUDED.parents_found_last_run;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                # ── Chat link helper ───────────────────────────────────

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION build_chat_link(
                        p_your_user_id  VARCHAR(30),
                        p_their_user_id VARCHAR(30)
                    )
                    RETURNS TEXT AS $$
                    BEGIN
                        IF p_your_user_id IS NULL OR p_their_user_id IS NULL THEN
                            RETURN NULL;
                        END IF;
                        RETURN 'https://x.com/i/chat/' || p_your_user_id || '-' || p_their_user_id;
                    END;
                    $$ LANGUAGE plpgsql IMMUTABLE;
                """)

                # ── Statistics helpers ─────────────────────────────────

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_extraction_statistics(p_account_id INTEGER)
                    RETURNS TABLE (
                        total_extracted BIGINT,
                        total_replies BIGINT,
                        total_regular BIGINT,
                        pending_parent_extraction BIGINT,
                        parents_found BIGINT,
                        parents_not_found BIGINT,
                        moved_to_links BIGINT,
                        pending_move_to_links BIGINT
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            COUNT(*)::BIGINT,
                            COUNT(CASE WHEN is_reply = TRUE THEN 1 END)::BIGINT,
                            COUNT(CASE WHEN is_reply = FALSE THEN 1 END)::BIGINT,
                            COUNT(CASE WHEN is_reply = TRUE AND parent_extraction_attempted = FALSE THEN 1 END)::BIGINT,
                            COUNT(CASE WHEN parent_extracted = TRUE THEN 1 END)::BIGINT,
                            COUNT(CASE WHEN is_reply = TRUE AND parent_extraction_attempted = TRUE AND parent_extracted = FALSE THEN 1 END)::BIGINT,
                            COUNT(CASE WHEN linked_to_links_table = TRUE THEN 1 END)::BIGINT,
                            COUNT(CASE WHEN linked_to_links_table = FALSE THEN 1 END)::BIGINT
                        FROM extracted_urls
                        WHERE account_id = p_account_id;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                # ── Prompt helpers ─────────────────────────────────────

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_latest_prompt_backup(p_prompt_id INTEGER)
                    RETURNS TABLE (
                        backup_id INTEGER,
                        version_number INTEGER,
                        prompt_content TEXT,
                        backed_up_at TIMESTAMP
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT pb.backup_id, pb.version_number, pb.prompt_content, pb.backed_up_at
                        FROM prompt_backups pb
                        WHERE pb.prompt_id = p_prompt_id
                        ORDER BY pb.version_number DESC
                        LIMIT 1;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_all_prompt_backups(
                        p_prompt_id INTEGER DEFAULT NULL,
                        p_username VARCHAR DEFAULT NULL,
                        p_prompt_type VARCHAR DEFAULT NULL
                    )
                    RETURNS TABLE (
                        backup_id INTEGER,
                        prompt_id INTEGER,
                        username VARCHAR,
                        prompt_name VARCHAR,
                        prompt_type VARCHAR,
                        version_number INTEGER,
                        backed_up_at TIMESTAMP,
                        backup_type VARCHAR,
                        is_restorable BOOLEAN
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            pb.backup_id, pb.prompt_id, pb.username, pb.prompt_name,
                            pb.prompt_type, pb.version_number, pb.backed_up_at,
                            pb.backup_type, pb.is_restorable
                        FROM prompt_backups pb
                        WHERE (p_prompt_id IS NULL OR pb.prompt_id = p_prompt_id)
                          AND (p_username IS NULL OR pb.username = p_username)
                          AND (p_prompt_type IS NULL OR pb.prompt_type = p_prompt_type)
                        ORDER BY pb.backed_up_at DESC;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_prompt_variations(
                        p_prompt_id INTEGER,
                        p_unused_only BOOLEAN DEFAULT FALSE
                    )
                    RETURNS TABLE (
                        variation_id INTEGER,
                        variation_number INTEGER,
                        variation_content TEXT,
                        used BOOLEAN,
                        created_at TIMESTAMP,
                        copied_count INTEGER
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            pv.variation_id, pv.variation_number, pv.variation_content,
                            pv.used, pv.created_at, pv.copied_count
                        FROM prompt_variations pv
                        WHERE pv.parent_prompt_id = p_prompt_id
                          AND pv.is_active = TRUE
                          AND (p_unused_only = FALSE OR pv.used = FALSE)
                        ORDER BY pv.variation_number;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION mark_variation_used(p_variation_id INTEGER)
                    RETURNS BOOLEAN AS $$
                    BEGIN
                        UPDATE prompt_variations
                        SET used = TRUE, used_at = CURRENT_TIMESTAMP
                        WHERE variation_id = p_variation_id;
                        RETURN FOUND;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION increment_variation_copy_count(p_variation_id INTEGER)
                    RETURNS BOOLEAN AS $$
                    BEGIN
                        UPDATE prompt_variations
                        SET copied_count   = copied_count + 1,
                            last_copied_at = CURRENT_TIMESTAMP
                        WHERE variation_id = p_variation_id;
                        RETURN FOUND;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_account_variation_stats(p_account_id INTEGER)
                    RETURNS TABLE (
                        prompt_type VARCHAR,
                        total_variations BIGINT,
                        unused_variations BIGINT,
                        used_variations BIGINT,
                        total_copied BIGINT
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            pv.prompt_type,
                            COUNT(*)::BIGINT,
                            COUNT(CASE WHEN pv.used = FALSE THEN 1 END)::BIGINT,
                            COUNT(CASE WHEN pv.used = TRUE THEN 1 END)::BIGINT,
                            SUM(pv.copied_count)::BIGINT
                        FROM prompt_variations pv
                        WHERE pv.account_id = p_account_id
                          AND pv.is_active = TRUE
                        GROUP BY pv.prompt_type;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                # ── Backward compatibility ─────────────────────────────

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_available_accounts()
                    RETURNS TABLE (
                        account_id INTEGER,
                        username VARCHAR,
                        profile_id VARCHAR,
                        created_time TIMESTAMP
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT a.account_id, a.username, a.profile_id, a.created_time
                        FROM accounts a
                        ORDER BY a.username;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                logger.info("✓ All helper functions ready (17 functions)")

                # ======================================================
                # STEP 6: CREATE VIEWS
                # PostgreSQL does not allow CREATE OR REPLACE VIEW to change
                # column names or add columns in a different position. We
                # therefore DROP each view first (CASCADE is safe — views
                # hold no data) and then recreate it fresh. This is
                # idempotent and harmless on a new database too.
                # ======================================================
                logger.info("Dropping old views (safe — no data) and recreating...")

                views_to_drop = [
                    "extracted_urls_pending_parent",
                    "extracted_urls_with_parents",
                    "parent_tweets_in_links",
                    "links_with_chat",
                    "extraction_state_summary",
                    "prompt_backup_summary",
                    "prompt_variations_summary",
                    "content_with_variations",
                ]
                for vname in views_to_drop:
                    cursor.execute(f"DROP VIEW IF EXISTS {vname} CASCADE;")
                logger.info(f"  ↳ Dropped {len(views_to_drop)} views (CASCADE)")

                # Child tweets awaiting parent extraction
                cursor.execute("""
                    CREATE VIEW extracted_urls_pending_parent AS
                    SELECT
                        eu.extracted_url_id,
                        eu.account_id,
                        a.username,
                        eu.url as child_url,
                        eu.tweet_id as child_tweet_id,
                        eu.is_reply,
                        eu.extracted_at,
                        eu.extraction_batch_id
                    FROM extracted_urls eu
                    JOIN accounts a ON eu.account_id = a.account_id
                    WHERE eu.is_reply = TRUE
                      AND eu.parent_extraction_attempted = FALSE
                    ORDER BY eu.extracted_at ASC;
                """)

                # Child tweets with parent information
                cursor.execute("""
                    CREATE VIEW extracted_urls_with_parents AS
                    SELECT
                        eu.extracted_url_id as child_id,
                        eu.account_id,
                        a.username,
                        eu.url as child_url,
                        eu.tweet_id as child_tweet_id,
                        eu.parent_extracted,
                        eu.parent_tweet_url,
                        eu.parent_tweet_id,
                        peu.url as parent_url_in_extracted,
                        l.links_id as parent_links_id,
                        l.link as parent_url_in_links,
                        eu.linked_to_links_table as parent_in_links_table,
                        eu.links_table_id,
                        eu.extracted_at
                    FROM extracted_urls eu
                    JOIN accounts a ON eu.account_id = a.account_id
                    LEFT JOIN extracted_urls peu ON eu.parent_url_id = peu.extracted_url_id
                    LEFT JOIN links l ON eu.links_table_id = l.links_id
                    WHERE eu.is_reply = TRUE
                    ORDER BY eu.extracted_at DESC;
                """)

                # Parent tweets in links table (includes new columns)
                cursor.execute("""
                    CREATE VIEW parent_tweets_in_links AS
                    SELECT
                        l.links_id as parent_links_id,
                        l.account_id,
                        a.username,
                        l.link as parent_url,
                        l.tweet_id as parent_tweet_id,
                        l.tweet_author_user_id,
                        l.chat_link,
                        l.extracted_url_id as source_child_id,
                        l.is_parent_tweet,
                        l.used,
                        l.filtered,
                        l.within_limit,
                        l.executed,
                        l.success,
                        l.failure,
                        l.scraped_time
                    FROM links l
                    JOIN accounts a ON l.account_id = a.account_id
                    WHERE l.is_parent_tweet = TRUE
                    ORDER BY l.scraped_time DESC;
                """)

                # Links with resolved chat URLs
                cursor.execute("""
                    CREATE VIEW links_with_chat AS
                    SELECT
                        l.links_id,
                        l.account_id,
                        a.username AS account_username,
                        l.link AS tweet_url,
                        l.tweet_id,
                        l.tweet_author_user_id,
                        l.chat_link,
                        l.tweeted_time,
                        l.used,
                        l.filtered,
                        l.executed,
                        l.success,
                        l.failure,
                        l.workflow_status
                    FROM links l
                    JOIN accounts a ON l.account_id = a.account_id
                    WHERE l.chat_link IS NOT NULL
                    ORDER BY l.tweeted_time DESC NULLS LAST;
                """)

                # Extraction state summary
                cursor.execute("""
                    CREATE VIEW extraction_state_summary AS
                    SELECT
                        aes.state_id,
                        aes.username,
                        aes.last_seen_tweet_id,
                        aes.last_extraction_time,
                        aes.tweets_found_last_run,
                        aes.parents_found_last_run,
                        aes.last_tweet_url,
                        ROUND(
                            EXTRACT(EPOCH FROM (NOW() - aes.last_extraction_time)) / 3600, 1
                        ) AS hours_since_last_extraction,
                        CASE
                            WHEN aes.last_extraction_time IS NULL THEN TRUE
                            WHEN NOW() - aes.last_extraction_time > INTERVAL '6 hours' THEN TRUE
                            ELSE FALSE
                        END AS is_stale,
                        aes.created_at,
                        aes.updated_at
                    FROM account_extraction_state aes
                    ORDER BY aes.last_extraction_time DESC NULLS LAST;
                """)

                # Prompt backup summary
                cursor.execute("""
                    CREATE VIEW prompt_backup_summary AS
                    SELECT
                        p.prompt_id,
                        p.account_id,
                        p.name as current_prompt_name,
                        p.prompt_type,
                        COUNT(pb.backup_id) as total_backups,
                        MAX(pb.version_number) as latest_version,
                        MIN(pb.backed_up_at) as first_backup,
                        MAX(pb.backed_up_at) as latest_backup,
                        COUNT(CASE WHEN pb.is_restorable = TRUE THEN 1 END) as restorable_backups,
                        COUNT(CASE WHEN pb.restored = TRUE THEN 1 END) as restored_count
                    FROM prompts p
                    LEFT JOIN prompt_backups pb ON p.prompt_id = pb.prompt_id
                    GROUP BY p.prompt_id, p.account_id, p.name, p.prompt_type;
                """)

                # Prompt variations summary
                cursor.execute("""
                    CREATE VIEW prompt_variations_summary AS
                    SELECT
                        p.prompt_id,
                        p.account_id,
                        a.username,
                        p.name as prompt_name,
                        p.prompt_type,
                        COUNT(pv.variation_id) as total_variations,
                        COUNT(CASE WHEN pv.used = FALSE THEN 1 END) as unused_variations,
                        COUNT(CASE WHEN pv.used = TRUE THEN 1 END) as used_variations,
                        SUM(pv.copied_count) as total_copies,
                        MAX(pv.created_at) as latest_generation,
                        COUNT(DISTINCT pv.generation_batch_id) as generation_batches
                    FROM prompts p
                    LEFT JOIN accounts a ON p.account_id = a.account_id
                    LEFT JOIN prompt_variations pv ON p.prompt_id = pv.parent_prompt_id
                    GROUP BY p.prompt_id, p.account_id, a.username, p.name, p.prompt_type;
                """)

                # Content with variations tracking
                cursor.execute("""
                    CREATE OR REPLACE VIEW content_with_variations AS
                    SELECT
                        c.content_id,
                        c.account_id,
                        c.content_name,
                        c.content_type,
                        c.used,
                        c.created_time,
                        pv.variation_id,
                        pv.variation_number,
                        pv.generation_batch_id,
                        p.prompt_id,
                        p.name as prompt_name
                    FROM content c
                    LEFT JOIN prompt_variations pv ON c.generated_from_variation_id = pv.variation_id
                    LEFT JOIN prompts p ON pv.parent_prompt_id = p.prompt_id;
                """)

                logger.info("✓ All views ready (8 views)")

                # ======================================================
                # COMMIT & FINISH
                # ======================================================
                conn.commit()
                logger.info("=" * 60)
                logger.info("✅ POSTGRESQL SCHEMA FULLY INITIALIZED")
                logger.info("=" * 60)
                logger.info("📊 Schema summary:")
                logger.info("  • 15 tables (incl. account_extraction_state)")
                logger.info("  • 17 helper functions")
                logger.info("  • 8 views (incl. links_with_chat, extraction_state_summary)")
                logger.info("  • 6 triggers (incl. extraction_state updated_at)")
                logger.info("  • ~55 indexes")
                logger.info("🆕 New columns in links table:")
                logger.info("  • success / failure (execution result tracking)")
                logger.info("  • tweet_author_user_id (numeric X user ID of tweet author)")
                logger.info("  • chat_link (https://x.com/i/chat/YOUR_ID-THEIR_ID)")
                logger.info("=" * 60)

                return True

    except Psycopg2Error as e:
        logger.error("=" * 60)
        logger.error("❌ PostgreSQL ERROR during schema init")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        if st:
            st.error(f"Database schema error: {e}")
        return False

    except Exception as e:
        logger.exception("=" * 60)
        logger.exception("❌ UNEXPECTED ERROR during schema init")
        logger.exception("=" * 60)
        if st:
            st.error(f"Unexpected error: {e}")
        return False


# PUBLIC API
def ensure_database():
    """Ensure database schema exists - creates if missing, preserves existing data."""
    return create_postgres_tables()


__all__ = ["ensure_database", "create_postgres_tables"]
