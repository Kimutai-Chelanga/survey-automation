import logging
from psycopg2 import Error as Psycopg2Error
import streamlit as st
from .connection import get_postgres_connection

logger = logging.getLogger(__name__)

# ===================================================================
# POSTGRESQL SCHEMA - SURVEY AUTOMATION ARCHITECTURE
# Updated: 2026-03-19
# Complete schema for survey automation with:
#   - Accounts with demographic fields
#   - Survey sites by name (not URL)
#   - Account URLs with usage tracking
#   - Questions with click elements and categories
#   - Answers with workflow tracking
#   - Prompts (one per user)
#   - Workflows with upload tracking
#   - Extraction state tracking
#   - Workflow generation logs
# ===================================================================


def create_postgres_tables():
    """
    Creates COMPLETE PostgreSQL schema for survey automation.
    ONLY creates tables if they don't exist - NEVER drops existing data.
    """

    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:

                # ======================================================
                # STEP 1: CREATE CORE TABLES (ONLY IF NOT EXISTS)
                # ======================================================
                logger.info("Creating SURVEY AUTOMATION schema...")

                # ACCOUNTS TABLE - WITH ALL DEMOGRAPHIC FIELDS
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        account_id SERIAL PRIMARY KEY,
                        username VARCHAR(255) NOT NULL UNIQUE,
                        country VARCHAR(100),
                        profile_id VARCHAR(255) UNIQUE,
                        profile_type VARCHAR(50) DEFAULT 'local_chrome'
                            CHECK (profile_type IN ('local_chrome', 'hyperbrowser')),
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        mongo_object_id VARCHAR(24),
                        total_surveys_processed INTEGER DEFAULT 0,
                        has_cookies BOOLEAN DEFAULT FALSE,
                        cookies_last_updated TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,

                        -- Demographic fields (all optional)
                        age INTEGER,
                        date_of_birth DATE,
                        gender VARCHAR(50),
                        city VARCHAR(100),
                        education_level VARCHAR(100),
                        email VARCHAR(255),
                        phone VARCHAR(50),
                        job_status VARCHAR(100),
                        industry VARCHAR(100),
                        income_range VARCHAR(50),
                        marital_status VARCHAR(50),
                        household_size INTEGER,
                        has_children BOOLEAN,
                        shopping_habits TEXT,
                        brands_used TEXT,
                        hobbies TEXT,
                        internet_usage VARCHAR(100),
                        device_type VARCHAR(100),
                        owns_laptop BOOLEAN,
                        owns_tv BOOLEAN,
                        internet_provider VARCHAR(100),
                        demographic_data JSONB
                    )
                """)
                logger.info("✓ accounts table ready (with demographic fields)")

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

                # SURVEY SITES TABLE - BY NAME, NOT URL
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS survey_sites (
                        site_id SERIAL PRIMARY KEY,
                        site_name VARCHAR(255) UNIQUE NOT NULL,
                        description TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                logger.info("✓ survey_sites table ready (by name)")

                # ACCOUNT URLS TABLE - PER-ACCOUNT URLS WITH USAGE TRACKING
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS account_urls (
                        url_id SERIAL PRIMARY KEY,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                        url TEXT NOT NULL,
                        is_default BOOLEAN DEFAULT FALSE,
                        is_used BOOLEAN DEFAULT FALSE,
                        used_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        notes TEXT,
                        UNIQUE(account_id, site_id, url)
                    )
                """)
                logger.info("✓ account_urls table ready (with usage tracking)")

                # PROMPTS TABLE - ONE PER USER
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS prompts (
                        prompt_id SERIAL PRIMARY KEY,
                        account_id INTEGER UNIQUE NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        name VARCHAR(255) NOT NULL,
                        content TEXT NOT NULL,
                        prompt_type VARCHAR(50) DEFAULT 'user_persona',
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        mongo_object_id VARCHAR(24),
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                logger.info("✓ prompts table ready (one per user)")

                # PROMPT BACKUPS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS prompt_backups (
                        backup_id SERIAL PRIMARY KEY,
                        prompt_id INTEGER NOT NULL REFERENCES prompts(prompt_id) ON DELETE CASCADE,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        username VARCHAR(255) NOT NULL,
                        prompt_name VARCHAR(255) NOT NULL,
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

                # WORKFLOWS TABLE - WITH UPLOAD TRACKING
                # Must be created BEFORE questions (questions references workflows)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS workflows (
                        workflow_id SERIAL PRIMARY KEY,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                        workflow_name VARCHAR(255) NOT NULL,
                        workflow_data JSONB,
                        question_id INTEGER,
                        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        uploaded_to_chrome BOOLEAN DEFAULT FALSE,
                        uploaded_at TIMESTAMP,
                        description TEXT
                    )
                """)
                logger.info("✓ workflows table ready (with upload tracking)")

                # QUESTIONS TABLE - WITH CLICK ELEMENTS AND CATEGORIES
                # workflow_id is safe here because workflows table already exists above
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS questions (
                        question_id SERIAL PRIMARY KEY,
                        survey_site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                        account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                        workflow_id INTEGER REFERENCES workflows(workflow_id) ON DELETE SET NULL,
                        question_text TEXT NOT NULL,
                        question_type VARCHAR(50) NOT NULL CHECK (question_type IN (
                            'multiple_choice', 'text', 'rating', 'yes_no', 'dropdown', 'checkbox', 'radio'
                        )),
                        question_category VARCHAR(100),
                        options JSONB,
                        click_element TEXT,
                        input_element TEXT,
                        submit_element TEXT,
                        required BOOLEAN DEFAULT TRUE,
                        order_index INTEGER DEFAULT 0,
                        page_url TEXT,
                        element_html TEXT,
                        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        extraction_batch_id VARCHAR(100),
                        used_in_workflow BOOLEAN DEFAULT FALSE,
                        used_at TIMESTAMP,
                        metadata JSONB,
                        CONSTRAINT unique_question_per_site UNIQUE (survey_site_id, question_text, account_id)
                    )
                """)
                logger.info("✓ questions table ready (with click elements and categories)")

                # ANSWERS TABLE
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS answers (
                        answer_id SERIAL PRIMARY KEY,
                        question_id INTEGER NOT NULL REFERENCES questions(question_id) ON DELETE CASCADE,
                        account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                        workflow_id INTEGER REFERENCES workflows(workflow_id) ON DELETE SET NULL,
                        answer_text TEXT,
                        answer_value_numeric NUMERIC,
                        answer_value_boolean BOOLEAN,
                        submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        submission_batch_id VARCHAR(100),
                        metadata JSONB
                    )
                """)
                logger.info("✓ answers table ready")

                # EXTRACTION STATE TABLE - WITH URL TRACKING
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS extraction_state (
                        state_id SERIAL PRIMARY KEY,
                        account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                        site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                        url_id INTEGER REFERENCES account_urls(url_id) ON DELETE SET NULL,
                        last_extraction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_extraction_batch_id VARCHAR(100),
                        questions_found_last_run INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(account_id, site_id)
                    )
                """)
                logger.info("✓ extraction_state table ready")

                # WORKFLOW GENERATION LOG
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_generation_log (
                        log_id SERIAL PRIMARY KEY,
                        workflow_type VARCHAR(50) NOT NULL,
                        workflow_name VARCHAR(255) NOT NULL,
                        account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
                        site_id INTEGER REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                        prompt_id INTEGER REFERENCES prompts(prompt_id) ON DELETE SET NULL,
                        workflow_id INTEGER REFERENCES workflows(workflow_id) ON DELETE SET NULL,
                        question_id INTEGER REFERENCES questions(question_id) ON DELETE SET NULL,
                        username VARCHAR(255),
                        generated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR(50) DEFAULT 'success'
                            CHECK (status IN ('success', 'failed', 'partial')),
                        questions_processed INTEGER DEFAULT 0,
                        answers_generated INTEGER DEFAULT 0,
                        error_message TEXT,
                        metadata JSONB
                    )
                """)
                logger.info("✓ workflow_generation_log table ready")

                # ======================================================
                # STEP 2: CREATE INDEXES
                # ======================================================
                logger.info("Creating indexes...")

                # Accounts
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounts_username ON accounts(username);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounts_country ON accounts(country);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active) WHERE is_active = TRUE;")

                # Account cookies
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_cookies_account ON account_cookies(account_id);")

                # Survey sites
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_survey_sites_name ON survey_sites(site_name);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_survey_sites_active ON survey_sites(is_active) WHERE is_active = TRUE;")

                # Account URLs
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_urls_account_site ON account_urls(account_id, site_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_urls_used ON account_urls(is_used) WHERE is_used = FALSE;")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_urls_default ON account_urls(is_default) WHERE is_default = TRUE;")

                # Prompts
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompts_account ON prompts(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_prompts_active ON prompts(is_active) WHERE is_active = TRUE;")

                # Prompt backups
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_backups_prompt ON prompt_backups(prompt_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_backups_account ON prompt_backups(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_backups_version ON prompt_backups(prompt_id, version_number);")

                # Workflows
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_workflows_account_site ON workflows(account_id, site_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_workflows_uploaded ON workflows(uploaded_to_chrome) WHERE uploaded_to_chrome = FALSE;")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_workflows_active ON workflows(is_active) WHERE is_active = TRUE;")

                # Questions
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_site ON questions(survey_site_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_account ON questions(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_workflow ON questions(workflow_id) WHERE workflow_id IS NOT NULL;")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_type ON questions(question_type);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_category ON questions(question_category);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_batch ON questions(extraction_batch_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_unused ON questions(used_in_workflow) WHERE used_in_workflow = FALSE;")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_active ON questions(is_active) WHERE is_active = TRUE;")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_questions_click_element ON questions(click_element) WHERE click_element IS NOT NULL;")

                # Answers
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_answers_question ON answers(question_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_answers_account ON answers(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_answers_workflow ON answers(workflow_id) WHERE workflow_id IS NOT NULL;")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_answers_batch ON answers(submission_batch_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_answers_submitted ON answers(submitted_at DESC);")

                # Extraction state
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extraction_account_site ON extraction_state(account_id, site_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_extraction_time ON extraction_state(last_extraction_time DESC);")

                # Workflow generation log
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_wf_log_account ON workflow_generation_log(account_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_wf_log_site ON workflow_generation_log(site_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_wf_log_workflow ON workflow_generation_log(workflow_id) WHERE workflow_id IS NOT NULL;")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_wf_log_time ON workflow_generation_log(generated_time DESC);")

                logger.info("✓ All indexes ready")

                # ======================================================
                # STEP 3: CREATE TRIGGER FUNCTIONS
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
                        v_username VARCHAR(255);
                    BEGIN
                        IF TG_OP = 'DELETE' THEN
                            v_backup_type := 'pre_delete';
                        ELSE
                            v_backup_type := 'pre_update';
                        END IF;

                        SELECT username INTO v_username
                        FROM accounts
                        WHERE account_id = OLD.account_id;

                        SELECT COALESCE(MAX(version_number), 0) + 1
                        INTO v_version_number
                        FROM prompt_backups
                        WHERE prompt_id = OLD.prompt_id;

                        INSERT INTO prompt_backups (
                            prompt_id, account_id, username, prompt_name, prompt_content,
                            version_number, backup_type, backup_reason, metadata
                        )
                        VALUES (
                            OLD.prompt_id, OLD.account_id, v_username, OLD.name, OLD.content,
                            v_version_number, v_backup_type,
                            CASE WHEN TG_OP = 'DELETE' THEN 'Automatic backup before deletion'
                                 ELSE 'Automatic backup before update' END,
                            jsonb_build_object(
                                'is_active', OLD.is_active,
                                'created_time', OLD.created_time,
                                'updated_time', OLD.updated_time
                            )
                        );
                        RETURN OLD;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                logger.info("✓ Trigger functions ready")

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
                    ("update_survey_sites_updated_time", "survey_sites", """
                        CREATE TRIGGER update_survey_sites_updated_time
                        BEFORE UPDATE ON survey_sites
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_time_column();
                    """),
                    ("update_workflows_updated_time", "workflows", """
                        CREATE TRIGGER update_workflows_updated_time
                        BEFORE UPDATE ON workflows
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_time_column();
                    """),
                    ("update_account_urls_updated_time", "account_urls", """
                        CREATE TRIGGER update_account_urls_updated_time
                        BEFORE UPDATE ON account_urls
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_time_column();
                    """),
                    ("trg_extraction_state_updated_at", "extraction_state", """
                        CREATE TRIGGER trg_extraction_state_updated_at
                        BEFORE UPDATE ON extraction_state
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

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_questions_by_site(p_site_id INTEGER)
                    RETURNS TABLE (
                        question_id INTEGER,
                        question_text TEXT,
                        question_type VARCHAR,
                        question_category VARCHAR,
                        options JSONB,
                        click_element TEXT,
                        input_element TEXT,
                        submit_element TEXT,
                        required BOOLEAN,
                        answer_count BIGINT,
                        used_in_workflow BOOLEAN
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            q.question_id,
                            q.question_text,
                            q.question_type,
                            q.question_category,
                            q.options,
                            q.click_element,
                            q.input_element,
                            q.submit_element,
                            q.required,
                            COUNT(a.answer_id)::BIGINT,
                            q.used_in_workflow
                        FROM questions q
                        LEFT JOIN answers a ON q.question_id = a.question_id
                        WHERE q.survey_site_id = p_site_id AND q.is_active = TRUE
                        GROUP BY q.question_id, q.question_text, q.question_type, q.question_category,
                                 q.options, q.click_element, q.input_element, q.submit_element, q.required,
                                 q.used_in_workflow
                        ORDER BY q.order_index, q.extracted_at;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_unused_questions(
                        p_account_id INTEGER DEFAULT NULL,
                        p_site_id INTEGER DEFAULT NULL
                    )
                    RETURNS TABLE (
                        question_id INTEGER,
                        question_text TEXT,
                        question_type VARCHAR,
                        question_category VARCHAR,
                        options JSONB,
                        click_element TEXT,
                        input_element TEXT,
                        submit_element TEXT,
                        survey_site_name VARCHAR
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            q.question_id,
                            q.question_text,
                            q.question_type,
                            q.question_category,
                            q.options,
                            q.click_element,
                            q.input_element,
                            q.submit_element,
                            ss.site_name
                        FROM questions q
                        JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                        WHERE (q.used_in_workflow IS NULL OR q.used_in_workflow = FALSE)
                          AND q.is_active = TRUE
                          AND (p_account_id IS NULL OR q.account_id = p_account_id)
                          AND (p_site_id IS NULL OR q.survey_site_id = p_site_id)
                        ORDER BY ss.site_name, q.question_category, q.extracted_at;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_answers_for_question(p_question_id INTEGER)
                    RETURNS TABLE (
                        answer_id INTEGER,
                        account_username VARCHAR,
                        answer_text TEXT,
                        answer_value_numeric NUMERIC,
                        answer_value_boolean BOOLEAN,
                        submitted_at TIMESTAMP,
                        submission_batch_id VARCHAR,
                        workflow_name VARCHAR
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            a.answer_id,
                            acc.username,
                            a.answer_text,
                            a.answer_value_numeric,
                            a.answer_value_boolean,
                            a.submitted_at,
                            a.submission_batch_id,
                            w.workflow_name
                        FROM answers a
                        LEFT JOIN accounts acc ON a.account_id = acc.account_id
                        LEFT JOIN workflows w ON a.workflow_id = w.workflow_id
                        WHERE a.question_id = p_question_id
                        ORDER BY a.submitted_at DESC;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_prompt_for_account(p_account_id INTEGER)
                    RETURNS TABLE (
                        prompt_id INTEGER,
                        prompt_name VARCHAR,
                        prompt_content TEXT,
                        is_active BOOLEAN
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT prompt_id, name, content, is_active
                        FROM prompts
                        WHERE account_id = p_account_id;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_workflows_by_site(
                        p_site_id INTEGER,
                        p_unused_only BOOLEAN DEFAULT TRUE
                    )
                    RETURNS TABLE (
                        workflow_id INTEGER,
                        workflow_name VARCHAR,
                        question_id INTEGER,
                        question_text TEXT,
                        created_time TIMESTAMP,
                        uploaded_to_chrome BOOLEAN,
                        uploaded_at TIMESTAMP
                    ) AS $$
                    BEGIN
                        RETURN QUERY
                        SELECT
                            w.workflow_id,
                            w.workflow_name,
                            w.question_id,
                            q.question_text,
                            w.created_time,
                            w.uploaded_to_chrome,
                            w.uploaded_at
                        FROM workflows w
                        LEFT JOIN questions q ON w.question_id = q.question_id
                        WHERE w.site_id = p_site_id
                          AND (p_unused_only = FALSE OR w.uploaded_to_chrome = FALSE)
                        ORDER BY w.created_time DESC;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION record_extraction_batch(
                        p_account_id INTEGER,
                        p_site_id INTEGER,
                        p_url_id INTEGER,
                        p_batch_id VARCHAR,
                        p_questions_found INTEGER
                    ) RETURNS VOID AS $$
                    BEGIN
                        INSERT INTO extraction_state (
                            account_id, site_id, url_id, last_extraction_time,
                            last_extraction_batch_id, questions_found_last_run
                        ) VALUES (
                            p_account_id, p_site_id, p_url_id, CURRENT_TIMESTAMP,
                            p_batch_id, p_questions_found
                        )
                        ON CONFLICT (account_id, site_id) DO UPDATE SET
                            last_extraction_time = CURRENT_TIMESTAMP,
                            last_extraction_batch_id = EXCLUDED.last_extraction_batch_id,
                            questions_found_last_run = EXCLUDED.questions_found_last_run,
                            url_id = EXCLUDED.url_id,
                            updated_at = CURRENT_TIMESTAMP;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                cursor.execute("""
                    CREATE OR REPLACE FUNCTION get_answer_statistics(p_question_id INTEGER)
                    RETURNS JSONB AS $$
                    DECLARE
                        v_question_type VARCHAR;
                        v_question_category VARCHAR;
                        v_result JSONB;
                    BEGIN
                        SELECT question_type, question_category INTO v_question_type, v_question_category
                        FROM questions
                        WHERE question_id = p_question_id;

                        IF v_question_type IN ('multiple_choice', 'dropdown', 'checkbox', 'radio') THEN
                            SELECT jsonb_build_object(
                                'type', v_question_type,
                                'category', v_question_category,
                                'total_answers', COUNT(*),
                                'breakdown', jsonb_object_agg(
                                    COALESCE(answer_text, 'unknown'),
                                    COUNT(*)
                                )
                            ) INTO v_result
                            FROM answers
                            WHERE question_id = p_question_id;

                        ELSIF v_question_type = 'rating' THEN
                            SELECT jsonb_build_object(
                                'type', v_question_type,
                                'category', v_question_category,
                                'total_answers', COUNT(*),
                                'average', AVG(answer_value_numeric),
                                'min', MIN(answer_value_numeric),
                                'max', MAX(answer_value_numeric),
                                'stddev', STDDEV(answer_value_numeric)
                            ) INTO v_result
                            FROM answers
                            WHERE question_id = p_question_id;

                        ELSIF v_question_type = 'yes_no' THEN
                            SELECT jsonb_build_object(
                                'type', v_question_type,
                                'category', v_question_category,
                                'total_answers', COUNT(*),
                                'yes_count', COUNT(*) FILTER (WHERE answer_value_boolean = TRUE),
                                'no_count', COUNT(*) FILTER (WHERE answer_value_boolean = FALSE)
                            ) INTO v_result
                            FROM answers
                            WHERE question_id = p_question_id;

                        ELSE
                            SELECT jsonb_build_object(
                                'type', v_question_type,
                                'category', v_question_category,
                                'total_answers', COUNT(*),
                                'unique_responses', COUNT(DISTINCT answer_text),
                                'avg_length', AVG(LENGTH(answer_text))
                            ) INTO v_result
                            FROM answers
                            WHERE question_id = p_question_id;
                        END IF;

                        RETURN v_result;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                logger.info("✓ Helper functions ready (7 functions)")

                # ======================================================
                # STEP 6: CREATE VIEWS
                # ======================================================
                logger.info("Creating views...")

                views_to_drop = [
                    "survey_site_summary",
                    "account_summary",
                    "question_stats",
                    "available_urls",
                    "available_questions",
                    "workflows_ready_for_upload",
                    "recent_extractions",
                    "prompt_backup_summary",
                    "workflow_summary",
                ]
                for vname in views_to_drop:
                    cursor.execute(f"DROP VIEW IF EXISTS {vname} CASCADE;")
                logger.info(f"  ↳ Dropped/refreshed {len(views_to_drop)} views")

                cursor.execute("""
                    CREATE VIEW survey_site_summary AS
                    SELECT
                        ss.site_id,
                        ss.site_name,
                        ss.description,
                        ss.created_at,
                        ss.is_active,
                        COUNT(DISTINCT q.question_id) as total_questions,
                        COUNT(DISTINCT CASE WHEN q.is_active THEN q.question_id END) as active_questions,
                        COUNT(DISTINCT q.account_id) as accounts_with_questions,
                        COUNT(DISTINCT a.answer_id) as total_answers,
                        COUNT(DISTINCT a.account_id) as unique_respondents,
                        COUNT(DISTINCT q.question_type) as type_count,
                        COUNT(DISTINCT q.question_category) as category_count,
                        COUNT(CASE WHEN q.click_element IS NOT NULL THEN 1 END) as questions_with_click,
                        COUNT(CASE WHEN q.used_in_workflow THEN 1 END) as questions_used,
                        COUNT(DISTINCT w.workflow_id) as total_workflows,
                        MIN(q.extracted_at) as first_question,
                        MAX(q.extracted_at) as latest_question,
                        COUNT(DISTINCT q.extraction_batch_id) as extraction_batches
                    FROM survey_sites ss
                    LEFT JOIN questions q ON ss.site_id = q.survey_site_id
                    LEFT JOIN answers a ON q.question_id = a.question_id
                    LEFT JOIN workflows w ON ss.site_id = w.site_id
                    GROUP BY ss.site_id, ss.site_name, ss.description, ss.created_at, ss.is_active;
                """)

                cursor.execute("""
                    CREATE VIEW account_summary AS
                    SELECT
                        a.account_id,
                        a.username,
                        a.country,
                        a.created_time,
                        a.is_active,
                        a.total_surveys_processed,
                        a.age,
                        a.gender,
                        a.city,
                        a.education_level,
                        a.job_status,
                        a.income_range,
                        COUNT(DISTINCT q.question_id) as questions_extracted,
                        COUNT(DISTINCT ans.answer_id) as answers_submitted,
                        COUNT(DISTINCT q.survey_site_id) as sites_participated,
                        COUNT(DISTINCT w.workflow_id) as workflows_created,
                        COUNT(DISTINCT au.url_id) as urls_configured,
                        COUNT(DISTINCT CASE WHEN au.is_used THEN au.url_id END) as urls_used,
                        MAX(ans.submitted_at) as last_answer,
                        p.prompt_id,
                        p.name as prompt_name,
                        p.is_active as prompt_active
                    FROM accounts a
                    LEFT JOIN questions q ON a.account_id = q.account_id
                    LEFT JOIN answers ans ON a.account_id = ans.account_id
                    LEFT JOIN workflows w ON a.account_id = w.account_id
                    LEFT JOIN account_urls au ON a.account_id = au.account_id
                    LEFT JOIN prompts p ON a.account_id = p.account_id
                    GROUP BY a.account_id, a.username, a.country, a.created_time, a.is_active,
                             a.total_surveys_processed, a.age, a.gender, a.city, a.education_level,
                             a.job_status, a.income_range, p.prompt_id, p.name, p.is_active;
                """)

                cursor.execute("""
                    CREATE VIEW question_stats AS
                    SELECT
                        q.question_id,
                        q.survey_site_id,
                        ss.site_name as survey_site_name,
                        q.account_id,
                        a.username,
                        q.question_text,
                        q.question_type,
                        q.question_category,
                        q.options,
                        q.click_element,
                        q.input_element,
                        q.submit_element,
                        q.required,
                        q.extracted_at,
                        q.is_active,
                        q.used_in_workflow,
                        q.used_at,
                        COUNT(DISTINCT ans.answer_id) as answer_count,
                        COUNT(DISTINCT ans.account_id) as unique_respondents,
                        MIN(ans.submitted_at) as first_answer,
                        MAX(ans.submitted_at) as last_answer,
                        q.extraction_batch_id
                    FROM questions q
                    LEFT JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                    LEFT JOIN accounts a ON q.account_id = a.account_id
                    LEFT JOIN answers ans ON q.question_id = ans.question_id
                    GROUP BY q.question_id, q.survey_site_id, ss.site_name, q.account_id, a.username,
                             q.question_text, q.question_type, q.question_category, q.options,
                             q.click_element, q.input_element, q.submit_element, q.required,
                             q.extracted_at, q.is_active, q.used_in_workflow, q.used_at,
                             q.extraction_batch_id;
                """)

                cursor.execute("""
                    CREATE VIEW available_urls AS
                    SELECT
                        au.url_id,
                        au.account_id,
                        a.username,
                        au.site_id,
                        ss.site_name,
                        au.url,
                        au.is_default,
                        au.is_used,
                        au.used_at,
                        au.created_at,
                        au.notes
                    FROM account_urls au
                    JOIN accounts a ON au.account_id = a.account_id
                    JOIN survey_sites ss ON au.site_id = ss.site_id
                    WHERE au.is_used = FALSE
                    ORDER BY au.is_default DESC, au.created_at DESC;
                """)

                cursor.execute("""
                    CREATE VIEW available_questions AS
                    SELECT
                        q.question_id,
                        q.survey_site_id,
                        ss.site_name,
                        q.account_id,
                        a.username,
                        q.question_text,
                        q.question_type,
                        q.question_category,
                        q.options,
                        q.click_element,
                        q.input_element,
                        q.submit_element,
                        q.required,
                        q.extracted_at
                    FROM questions q
                    JOIN accounts a ON q.account_id = a.account_id
                    JOIN survey_sites ss ON q.survey_site_id = ss.site_id
                    WHERE q.used_in_workflow = FALSE AND q.is_active = TRUE
                    ORDER BY ss.site_name, q.question_category, q.extracted_at DESC;
                """)

                cursor.execute("""
                    CREATE VIEW workflows_ready_for_upload AS
                    SELECT
                        w.workflow_id,
                        w.workflow_name,
                        w.account_id,
                        a.username,
                        w.site_id,
                        ss.site_name,
                        w.created_time,
                        w.question_id,
                        q.question_text,
                        q.question_type,
                        q.question_category,
                        q.click_element
                    FROM workflows w
                    JOIN accounts a ON w.account_id = a.account_id
                    JOIN survey_sites ss ON w.site_id = ss.site_id
                    LEFT JOIN questions q ON w.question_id = q.question_id
                    WHERE w.uploaded_to_chrome = FALSE AND w.is_active = TRUE
                    ORDER BY w.created_time DESC;
                """)

                cursor.execute("""
                    CREATE VIEW recent_extractions AS
                    SELECT
                        extraction_batch_id,
                        COUNT(*) as question_count,
                        MIN(extracted_at) as first_extracted,
                        MAX(extracted_at) as last_extracted,
                        COUNT(DISTINCT survey_site_id) as site_count,
                        COUNT(DISTINCT account_id) as account_count,
                        COUNT(DISTINCT question_type) as type_count,
                        COUNT(DISTINCT question_category) as category_count,
                        COUNT(CASE WHEN click_element IS NOT NULL THEN 1 END) as questions_with_click
                    FROM questions
                    WHERE extraction_batch_id IS NOT NULL
                    GROUP BY extraction_batch_id
                    ORDER BY last_extracted DESC;
                """)

                cursor.execute("""
                    CREATE VIEW prompt_backup_summary AS
                    SELECT
                        p.prompt_id,
                        p.account_id,
                        a.username,
                        p.name as current_prompt_name,
                        COUNT(pb.backup_id) as total_backups,
                        MAX(pb.version_number) as latest_version,
                        MIN(pb.backed_up_at) as first_backup,
                        MAX(pb.backed_up_at) as latest_backup
                    FROM prompts p
                    LEFT JOIN accounts a ON p.account_id = a.account_id
                    LEFT JOIN prompt_backups pb ON p.prompt_id = pb.prompt_id
                    GROUP BY p.prompt_id, p.account_id, a.username, p.name;
                """)

                cursor.execute("""
                    CREATE VIEW workflow_summary AS
                    SELECT
                        w.workflow_id,
                        w.workflow_name,
                        ss.site_name,
                        w.account_id,
                        a.username,
                        COUNT(DISTINCT q.question_id) as questions_processed,
                        COUNT(DISTINCT ans.answer_id) as answers_generated,
                        w.created_time,
                        w.updated_time,
                        w.uploaded_to_chrome,
                        w.uploaded_at,
                        w.is_active
                    FROM workflows w
                    LEFT JOIN survey_sites ss ON w.site_id = ss.site_id
                    LEFT JOIN accounts a ON w.account_id = a.account_id
                    LEFT JOIN questions q ON w.workflow_id = q.workflow_id
                    LEFT JOIN answers ans ON w.workflow_id = ans.workflow_id
                    GROUP BY w.workflow_id, w.workflow_name, ss.site_name, w.account_id, a.username,
                             w.created_time, w.updated_time, w.uploaded_to_chrome, w.uploaded_at, w.is_active;
                """)

                logger.info("✓ All views ready (9 views)")

                # ======================================================
                # STEP 7: INSERT DEFAULT DATA
                # ======================================================
                logger.info("Inserting default data...")

                cursor.execute("""
                    INSERT INTO survey_sites (site_name, description)
                    SELECT * FROM (VALUES
                        ('Top Surveys', 'General survey site with high-paying opportunities'),
                        ('Quick Rewards', 'Fast surveys with instant payouts'),
                        ('Survey Junkie', 'Popular survey platform'),
                        ('Pinecone Research', 'Product testing and surveys'),
                        ('Swagbucks', 'Earn points for surveys and activities'),
                        ('InboxDollars', 'Paid surveys and offers'),
                        ('MyPoints', 'Surveys and shopping rewards'),
                        ('Toluna', 'Community-based surveys'),
                        ('YouGov', 'Opinion surveys on current events'),
                        ('Vindale Research', 'High-paying survey site'),
                        ('PrizeRebel', 'Surveys and offers platform'),
                        ('LifePoints', 'Mobile and web surveys'),
                        ('SurveyMonkey Rewards', 'Paid survey platform'),
                        ('OnePoll', 'Daily news and opinion surveys'),
                        ('Valued Opinions', 'Consumer opinion surveys')
                    ) AS v(site_name, description)
                    WHERE NOT EXISTS (SELECT 1 FROM survey_sites LIMIT 1);
                """)

                logger.info("✓ Default survey sites inserted")

                # ======================================================
                # COMMIT & FINISH
                # ======================================================
                conn.commit()
                logger.info("=" * 60)
                logger.info("✅ POSTGRESQL SCHEMA FULLY INITIALIZED")
                logger.info("=" * 60)
                logger.info("📊 Schema summary:")
                logger.info("  • 11 tables:")
                logger.info("    - accounts (with 20+ demographic fields)")
                logger.info("    - account_cookies")
                logger.info("    - survey_sites (by name)")
                logger.info("    - account_urls (with usage tracking)")
                logger.info("    - prompts (one per user - UNIQUE constraint)")
                logger.info("    - prompt_backups")
                logger.info("    - workflows (with upload tracking)")
                logger.info("    - questions (with click elements and categories)")
                logger.info("    - answers (linked to workflows)")
                logger.info("    - extraction_state (with URL tracking)")
                logger.info("    - workflow_generation_log")
                logger.info("  • 7 helper functions")
                logger.info("  • 9 views")
                logger.info("  • 8 triggers")
                logger.info("  • ~40 indexes")
                logger.info("=" * 60)
                logger.info("🎯 Features:")
                logger.info("   - Click elements stored per question")
                logger.info("   - Question categories for better organization")
                logger.info("   - URL usage tracking (is_used flag)")
                logger.info("   - Workflow upload tracking to Chrome")
                logger.info("   - Demographic fields for rich prompts")
                logger.info("   - Survey sites by name (not URL)")
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