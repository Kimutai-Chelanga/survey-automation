-- =====================================================
-- PostgreSQL Schema - SURVEY AUTOMATION ARCHITECTURE
-- Updated: 2026-03-26 (with new account_cookies table)
-- Complete schema for survey automation with:
--   - Accounts with demographic fields
--   - Survey sites by name
--   - Account URLs with usage tracking
--   - Questions with click elements, categories, survey_name tracking
--   - Answers with workflow tracking
--   - Prompts (one per user)
--   - Workflows with upload tracking
--   - Extraction state tracking
--   - Workflow generation logs
--   - Screening results tracking (pass/fail per survey attempt)
--   - Proxy configurations per account
--   - Account cookies (new simplified structure)
-- =====================================================

-- ============= ACCOUNTS TABLE =============
CREATE TABLE IF NOT EXISTS accounts (
    account_id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(100),
    profile_id VARCHAR(255) UNIQUE,
    profile_type VARCHAR(50) DEFAULT 'local_chrome'
        CHECK (profile_type IN ('local_chrome', 'hyperbrowser', 'cloud')),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mongo_object_id VARCHAR(24),
    total_surveys_processed INTEGER DEFAULT 0,
    has_cookies BOOLEAN DEFAULT FALSE,
    cookies_last_updated TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    cloud_profile_id VARCHAR(100),
    active_proxy_id INTEGER,

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
);

COMMENT ON TABLE accounts IS 'User accounts for survey automation';
COMMENT ON COLUMN accounts.country IS 'Country for survey targeting and site selection';
COMMENT ON COLUMN accounts.cloud_profile_id IS 'Browser-use cloud profile ID';
COMMENT ON COLUMN accounts.active_proxy_id IS 'Currently active proxy configuration for this account';

-- ============= PROXY CONFIGURATIONS TABLE =============
CREATE TABLE IF NOT EXISTS proxy_configs (
    proxy_id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    proxy_type VARCHAR(10) NOT NULL CHECK (proxy_type IN ('http', 'https', 'socks4', 'socks5')),
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    username VARCHAR(255),
    password VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE proxy_configs IS 'Proxy configurations per account';
COMMENT ON COLUMN proxy_configs.proxy_type IS 'Proxy protocol: http, https, socks4, socks5';

-- ============= ACCOUNT COOKIES TABLE (UPDATED) =============
CREATE TABLE IF NOT EXISTS account_cookies (
    cookie_id    SERIAL PRIMARY KEY,
    account_id   INTEGER NOT NULL
                 REFERENCES accounts(account_id) ON DELETE CASCADE,
    domain       VARCHAR(255) NOT NULL DEFAULT 'google.com',
    cookies_json TEXT        NOT NULL,
    captured_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (account_id, domain)
);

CREATE INDEX IF NOT EXISTS idx_account_cookies_account_id
    ON account_cookies(account_id);

COMMENT ON TABLE account_cookies IS 'Stores cookie data per account and domain';
COMMENT ON COLUMN account_cookies.cookies_json IS 'JSON string of cookie data';

-- ============= SURVEY SITES TABLE =============
CREATE TABLE IF NOT EXISTS survey_sites (
    site_id SERIAL PRIMARY KEY,
    site_name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

COMMENT ON TABLE survey_sites IS 'Survey websites by name';
COMMENT ON COLUMN survey_sites.site_name IS 'Name of the survey site (e.g., "Top Surveys")';

-- ============= ACCOUNT URLS TABLE =============
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
);

COMMENT ON TABLE account_urls IS 'Per-account URLs for survey sites';
COMMENT ON COLUMN account_urls.is_used IS 'Whether this URL has been used for extraction';

-- ============= PROMPTS TABLE =============
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
);

COMMENT ON TABLE prompts IS 'One prompt per user defining their persona for answering questions';
COMMENT ON COLUMN prompts.account_id IS 'Unique - each user has exactly one prompt';

-- ============= PROMPT BACKUPS TABLE =============
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
);

-- ============= WORKFLOWS TABLE =============
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
);

COMMENT ON TABLE workflows IS 'Workflows created from questions';
COMMENT ON COLUMN workflows.uploaded_to_chrome IS 'Whether workflow has been uploaded to Chrome/Automa';

-- ============= QUESTIONS TABLE =============
CREATE TABLE IF NOT EXISTS questions (
    question_id SERIAL PRIMARY KEY,
    survey_site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
    account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
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
    workflow_id INTEGER,
    metadata JSONB,

    -- Survey tracking
    survey_name VARCHAR(255),
    survey_complete BOOLEAN DEFAULT FALSE,
    survey_completed_at TIMESTAMP,

    CONSTRAINT unique_question_per_site UNIQUE (survey_site_id, question_text, account_id)
);

COMMENT ON TABLE questions IS 'Questions extracted from survey sites';
COMMENT ON COLUMN questions.question_category IS 'Category classification of the question';
COMMENT ON COLUMN questions.click_element IS 'CSS selector or XPath to click on this question element';
COMMENT ON COLUMN questions.input_element IS 'CSS selector for input field (for text questions)';
COMMENT ON COLUMN questions.submit_element IS 'CSS selector for submit button';
COMMENT ON COLUMN questions.used_in_workflow IS 'Whether this question has been used in a workflow';
COMMENT ON COLUMN questions.survey_name IS 'Name of the survey this question was extracted from';
COMMENT ON COLUMN questions.survey_complete IS 'TRUE once the full survey — screener + body — has been completed';

-- ============= ANSWERS TABLE =============
CREATE TABLE IF NOT EXISTS answers (
    answer_id SERIAL PRIMARY KEY,
    question_id INTEGER NOT NULL REFERENCES questions(question_id) ON DELETE CASCADE,
    account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
    answer_text TEXT,
    answer_value_numeric NUMERIC,
    answer_value_boolean BOOLEAN,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    submission_batch_id VARCHAR(100),
    metadata JSONB,
    workflow_id INTEGER REFERENCES workflows(workflow_id) ON DELETE SET NULL
);

COMMENT ON TABLE answers IS 'Answers submitted by accounts to survey questions';
COMMENT ON COLUMN answers.workflow_id IS 'Workflow that submitted this answer';

-- ============= EXTRACTION STATE TABLE =============
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
);

COMMENT ON TABLE extraction_state IS 'Track last extraction per account and survey site';

-- ============= WORKFLOW GENERATION LOG =============
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
);

COMMENT ON TABLE workflow_generation_log IS 'Track manual workflow generation events';

-- ============= SCREENING RESULTS TABLE =============
CREATE TABLE IF NOT EXISTS screening_results (
    result_id        SERIAL PRIMARY KEY,
    account_id       INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    site_id          INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
    survey_name      VARCHAR(255),
    batch_id         VARCHAR(100),
    workflow_id      INTEGER REFERENCES workflows(workflow_id) ON DELETE SET NULL,
    status           VARCHAR(50) NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending', 'passed', 'failed', 'complete', 'error')),
    screener_answers INTEGER DEFAULT 0,
    survey_answers   INTEGER DEFAULT 0,
    started_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at     TIMESTAMP,
    notes            TEXT,
    metadata         JSONB
);

COMMENT ON TABLE screening_results IS 'One row per survey attempt — tracks pass/fail/complete per account per survey';
COMMENT ON COLUMN screening_results.screener_answers IS 'How many screener questions were answered by AI';
COMMENT ON COLUMN screening_results.survey_answers IS 'How many post-screener survey questions were answered';
COMMENT ON COLUMN screening_results.status IS 'pending=not yet run | passed=screener passed | failed=DQ | complete=full survey done';

-- ============= ADD FOREIGN KEY FOR accounts.active_proxy_id =============
ALTER TABLE accounts
    ADD CONSTRAINT fk_accounts_proxy
    FOREIGN KEY (active_proxy_id) REFERENCES proxy_configs(proxy_id) ON DELETE SET NULL;

-- ============= ADD FOREIGN KEY FOR questions.workflow_id =============
ALTER TABLE questions
    ADD CONSTRAINT fk_questions_workflow
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE SET NULL;

-- ============= INDEXES =============

-- Accounts
CREATE INDEX IF NOT EXISTS idx_accounts_username ON accounts(username);
CREATE INDEX IF NOT EXISTS idx_accounts_country ON accounts(country);
CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_accounts_cloud_profile ON accounts(cloud_profile_id) WHERE cloud_profile_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_accounts_active_proxy ON accounts(active_proxy_id) WHERE active_proxy_id IS NOT NULL;

-- Proxy configs
CREATE INDEX IF NOT EXISTS idx_proxy_account ON proxy_configs(account_id);
CREATE INDEX IF NOT EXISTS idx_proxy_active ON proxy_configs(is_active) WHERE is_active = TRUE;

-- Account cookies
-- Index already created with table

-- Survey sites
CREATE INDEX IF NOT EXISTS idx_survey_sites_name ON survey_sites(site_name);
CREATE INDEX IF NOT EXISTS idx_survey_sites_active ON survey_sites(is_active) WHERE is_active = TRUE;

-- Account URLs
CREATE INDEX IF NOT EXISTS idx_account_urls_account_site ON account_urls(account_id, site_id);
CREATE INDEX IF NOT EXISTS idx_account_urls_used ON account_urls(is_used) WHERE is_used = FALSE;
CREATE INDEX IF NOT EXISTS idx_account_urls_default ON account_urls(is_default) WHERE is_default = TRUE;

-- Prompts
CREATE INDEX IF NOT EXISTS idx_prompts_account ON prompts(account_id);
CREATE INDEX IF NOT EXISTS idx_prompts_active ON prompts(is_active) WHERE is_active = TRUE;

-- Prompt backups
CREATE INDEX IF NOT EXISTS idx_backups_prompt ON prompt_backups(prompt_id);
CREATE INDEX IF NOT EXISTS idx_backups_account ON prompt_backups(account_id);
CREATE INDEX IF NOT EXISTS idx_backups_version ON prompt_backups(prompt_id, version_number);

-- Workflows
CREATE INDEX IF NOT EXISTS idx_workflows_account_site ON workflows(account_id, site_id);
CREATE INDEX IF NOT EXISTS idx_workflows_uploaded ON workflows(uploaded_to_chrome) WHERE uploaded_to_chrome = FALSE;
CREATE INDEX IF NOT EXISTS idx_workflows_active ON workflows(is_active) WHERE is_active = TRUE;

-- Questions
CREATE INDEX IF NOT EXISTS idx_questions_site ON questions(survey_site_id);
CREATE INDEX IF NOT EXISTS idx_questions_account ON questions(account_id);
CREATE INDEX IF NOT EXISTS idx_questions_type ON questions(question_type);
CREATE INDEX IF NOT EXISTS idx_questions_category ON questions(question_category);
CREATE INDEX IF NOT EXISTS idx_questions_batch ON questions(extraction_batch_id);
CREATE INDEX IF NOT EXISTS idx_questions_unused ON questions(used_in_workflow) WHERE used_in_workflow = FALSE;
CREATE INDEX IF NOT EXISTS idx_questions_active ON questions(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_questions_click_element ON questions(click_element) WHERE click_element IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_questions_workflow_id ON questions(workflow_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_questions_survey_name ON questions(survey_name) WHERE survey_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_questions_survey_complete ON questions(survey_complete) WHERE survey_complete = FALSE;

-- Answers
CREATE INDEX IF NOT EXISTS idx_answers_question ON answers(question_id);
CREATE INDEX IF NOT EXISTS idx_answers_account ON answers(account_id);
CREATE INDEX IF NOT EXISTS idx_answers_batch ON answers(submission_batch_id);
CREATE INDEX IF NOT EXISTS idx_answers_submitted ON answers(submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_answers_workflow ON answers(workflow_id) WHERE workflow_id IS NOT NULL;

-- Extraction state
CREATE INDEX IF NOT EXISTS idx_extraction_account_site ON extraction_state(account_id, site_id);
CREATE INDEX IF NOT EXISTS idx_extraction_time ON extraction_state(last_extraction_time DESC);

-- Workflow generation log
CREATE INDEX IF NOT EXISTS idx_wf_log_account ON workflow_generation_log(account_id);
CREATE INDEX IF NOT EXISTS idx_wf_log_site ON workflow_generation_log(site_id);
CREATE INDEX IF NOT EXISTS idx_wf_log_workflow ON workflow_generation_log(workflow_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wf_log_time ON workflow_generation_log(generated_time DESC);

-- Screening results
CREATE INDEX IF NOT EXISTS idx_screening_account ON screening_results(account_id);
CREATE INDEX IF NOT EXISTS idx_screening_site ON screening_results(site_id);
CREATE INDEX IF NOT EXISTS idx_screening_status ON screening_results(status);
CREATE INDEX IF NOT EXISTS idx_screening_survey ON screening_results(survey_name);
CREATE INDEX IF NOT EXISTS idx_screening_batch ON screening_results(batch_id);

-- ============= TRIGGER FUNCTIONS =============

CREATE OR REPLACE FUNCTION update_updated_time_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_extraction_state_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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
        CASE WHEN TG_OP = 'DELETE'
             THEN 'Automatic backup before deletion'
             ELSE 'Automatic backup before update'
        END,
        jsonb_build_object(
            'is_active',    OLD.is_active,
            'created_time', OLD.created_time,
            'updated_time', OLD.updated_time
        )
    );

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- ============= TRIGGERS =============

DROP TRIGGER IF EXISTS update_accounts_updated_time ON accounts;
CREATE TRIGGER update_accounts_updated_time
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_time_column();

DROP TRIGGER IF EXISTS update_proxy_configs_updated_at ON proxy_configs;
CREATE TRIGGER update_proxy_configs_updated_at
    BEFORE UPDATE ON proxy_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_prompts_updated_time ON prompts;
CREATE TRIGGER update_prompts_updated_time
    BEFORE UPDATE ON prompts
    FOR EACH ROW EXECUTE FUNCTION update_updated_time_column();

DROP TRIGGER IF EXISTS backup_prompt_before_update ON prompts;
CREATE TRIGGER backup_prompt_before_update
    BEFORE UPDATE ON prompts
    FOR EACH ROW
    WHEN (OLD.content IS DISTINCT FROM NEW.content OR OLD.name IS DISTINCT FROM NEW.name)
    EXECUTE FUNCTION auto_backup_prompt();

DROP TRIGGER IF EXISTS backup_prompt_before_delete ON prompts;
CREATE TRIGGER backup_prompt_before_delete
    BEFORE DELETE ON prompts
    FOR EACH ROW EXECUTE FUNCTION auto_backup_prompt();

DROP TRIGGER IF EXISTS update_survey_sites_updated_time ON survey_sites;
CREATE TRIGGER update_survey_sites_updated_time
    BEFORE UPDATE ON survey_sites
    FOR EACH ROW EXECUTE FUNCTION update_updated_time_column();

DROP TRIGGER IF EXISTS update_workflows_updated_time ON workflows;
CREATE TRIGGER update_workflows_updated_time
    BEFORE UPDATE ON workflows
    FOR EACH ROW EXECUTE FUNCTION update_updated_time_column();

DROP TRIGGER IF EXISTS update_account_urls_updated_time ON account_urls;
CREATE TRIGGER update_account_urls_updated_time
    BEFORE UPDATE ON account_urls
    FOR EACH ROW EXECUTE FUNCTION update_updated_time_column();

DROP TRIGGER IF EXISTS trg_extraction_state_updated_at ON extraction_state;
CREATE TRIGGER trg_extraction_state_updated_at
    BEFORE UPDATE ON extraction_state
    FOR EACH ROW EXECUTE FUNCTION update_extraction_state_updated_at();

-- ============= HELPER FUNCTIONS =============

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
    used_in_workflow BOOLEAN,
    survey_name VARCHAR
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
        q.used_in_workflow,
        q.survey_name
    FROM questions q
    LEFT JOIN answers a ON q.question_id = a.question_id
    WHERE q.survey_site_id = p_site_id AND q.is_active = TRUE
    GROUP BY q.question_id, q.question_text, q.question_type, q.question_category,
             q.options, q.click_element, q.input_element, q.submit_element,
             q.required, q.used_in_workflow, q.survey_name
    ORDER BY q.survey_name, q.order_index, q.extracted_at;
END;
$$ LANGUAGE plpgsql;

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
    survey_site_name VARCHAR,
    survey_name VARCHAR
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
        ss.site_name,
        q.survey_name
    FROM questions q
    JOIN survey_sites ss ON q.survey_site_id = ss.site_id
    WHERE (q.used_in_workflow IS NULL OR q.used_in_workflow = FALSE)
      AND q.is_active = TRUE
      AND (p_account_id IS NULL OR q.account_id = p_account_id)
      AND (p_site_id IS NULL OR q.survey_site_id = p_site_id)
    ORDER BY ss.site_name, q.survey_name, q.question_category, q.extracted_at;
END;
$$ LANGUAGE plpgsql;

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
        last_extraction_time         = CURRENT_TIMESTAMP,
        last_extraction_batch_id     = EXCLUDED.last_extraction_batch_id,
        questions_found_last_run     = EXCLUDED.questions_found_last_run,
        url_id                       = EXCLUDED.url_id,
        updated_at                   = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_answer_statistics(p_question_id INTEGER)
RETURNS JSONB AS $$
DECLARE
    v_question_type VARCHAR;
    v_question_category VARCHAR;
    v_result JSONB;
BEGIN
    SELECT question_type, question_category
    INTO v_question_type, v_question_category
    FROM questions
    WHERE question_id = p_question_id;

    IF v_question_type IN ('multiple_choice', 'dropdown', 'checkbox', 'radio') THEN
        SELECT jsonb_build_object(
            'type',         v_question_type,
            'category',     v_question_category,
            'total_answers', COUNT(*),
            'breakdown',    jsonb_object_agg(COALESCE(answer_text, 'unknown'), COUNT(*))
        ) INTO v_result
        FROM answers
        WHERE question_id = p_question_id
        GROUP BY answer_text;

    ELSIF v_question_type = 'rating' THEN
        SELECT jsonb_build_object(
            'type',          v_question_type,
            'category',      v_question_category,
            'total_answers', COUNT(*),
            'average',       AVG(answer_value_numeric),
            'min',           MIN(answer_value_numeric),
            'max',           MAX(answer_value_numeric),
            'stddev',        STDDEV(answer_value_numeric)
        ) INTO v_result
        FROM answers
        WHERE question_id = p_question_id;

    ELSIF v_question_type = 'yes_no' THEN
        SELECT jsonb_build_object(
            'type',          v_question_type,
            'category',      v_question_category,
            'total_answers', COUNT(*),
            'yes_count',     COUNT(*) FILTER (WHERE answer_value_boolean = TRUE),
            'no_count',      COUNT(*) FILTER (WHERE answer_value_boolean = FALSE)
        ) INTO v_result
        FROM answers
        WHERE question_id = p_question_id;

    ELSE
        SELECT jsonb_build_object(
            'type',             v_question_type,
            'category',         v_question_category,
            'total_answers',    COUNT(*),
            'unique_responses', COUNT(DISTINCT answer_text),
            'avg_length',       AVG(LENGTH(answer_text))
        ) INTO v_result
        FROM answers
        WHERE question_id = p_question_id;
    END IF;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- ============= VIEWS =============

DROP VIEW IF EXISTS survey_site_summary CASCADE;
CREATE VIEW survey_site_summary AS
SELECT
    ss.site_id,
    ss.site_name,
    ss.description,
    ss.created_at,
    ss.is_active,
    COUNT(DISTINCT q.question_id)                                        AS total_questions,
    COUNT(DISTINCT CASE WHEN q.is_active THEN q.question_id END)        AS active_questions,
    COUNT(DISTINCT q.account_id)                                         AS accounts_with_questions,
    COUNT(DISTINCT a.answer_id)                                          AS total_answers,
    COUNT(DISTINCT a.account_id)                                         AS unique_respondents,
    COUNT(DISTINCT q.question_type)                                      AS type_count,
    COUNT(DISTINCT q.question_category)                                  AS category_count,
    COUNT(DISTINCT q.survey_name)                                        AS unique_surveys,
    COUNT(CASE WHEN q.click_element IS NOT NULL THEN 1 END)             AS questions_with_click,
    COUNT(CASE WHEN q.used_in_workflow THEN 1 END)                      AS questions_used,
    COUNT(DISTINCT w.workflow_id)                                        AS total_workflows,
    MIN(q.extracted_at)                                                  AS first_question,
    MAX(q.extracted_at)                                                  AS latest_question,
    COUNT(DISTINCT q.extraction_batch_id)                                AS extraction_batches
FROM survey_sites ss
LEFT JOIN questions q  ON ss.site_id = q.survey_site_id
LEFT JOIN answers a    ON q.question_id = a.question_id
LEFT JOIN workflows w  ON ss.site_id = w.site_id
GROUP BY ss.site_id, ss.site_name, ss.description, ss.created_at, ss.is_active;

COMMENT ON VIEW survey_site_summary IS 'Summary statistics per survey site';

DROP VIEW IF EXISTS account_summary CASCADE;
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
    a.cloud_profile_id,
    COUNT(DISTINCT q.question_id)                                        AS questions_extracted,
    COUNT(DISTINCT ans.answer_id)                                        AS answers_submitted,
    COUNT(DISTINCT q.survey_site_id)                                     AS sites_participated,
    COUNT(DISTINCT w.workflow_id)                                        AS workflows_created,
    COUNT(DISTINCT au.url_id)                                            AS urls_configured,
    COUNT(DISTINCT CASE WHEN au.is_used THEN au.url_id END)             AS urls_used,
    MAX(ans.submitted_at)                                                AS last_answer,
    p.prompt_id,
    p.name                                                               AS prompt_name,
    p.is_active                                                          AS prompt_active,
    pc.proxy_id,
    pc.proxy_type,
    pc.host,
    pc.port
FROM accounts a
LEFT JOIN questions q       ON a.account_id = q.account_id
LEFT JOIN answers ans       ON a.account_id = ans.account_id
LEFT JOIN workflows w       ON a.account_id = w.account_id
LEFT JOIN account_urls au   ON a.account_id = au.account_id
LEFT JOIN prompts p         ON a.account_id = p.account_id
LEFT JOIN proxy_configs pc  ON a.active_proxy_id = pc.proxy_id AND pc.is_active = TRUE
GROUP BY a.account_id, a.username, a.country, a.created_time, a.is_active,
         a.total_surveys_processed, a.age, a.gender, a.city, a.education_level,
         a.job_status, a.income_range, a.cloud_profile_id,
         p.prompt_id, p.name, p.is_active,
         pc.proxy_id, pc.proxy_type, pc.host, pc.port;

COMMENT ON VIEW account_summary IS 'Summary statistics per account with demographic info and proxy config';

DROP VIEW IF EXISTS question_stats CASCADE;
CREATE VIEW question_stats AS
SELECT
    q.question_id,
    q.survey_site_id,
    ss.site_name                        AS survey_site_name,
    q.account_id,
    a.username,
    q.question_text,
    q.question_type,
    q.question_category,
    q.survey_name,
    q.survey_complete,
    q.options,
    q.click_element,
    q.input_element,
    q.submit_element,
    q.required,
    q.extracted_at,
    q.is_active,
    q.used_in_workflow,
    q.used_at,
    COUNT(DISTINCT ans.answer_id)       AS answer_count,
    COUNT(DISTINCT ans.account_id)      AS unique_respondents,
    MIN(ans.submitted_at)               AS first_answer,
    MAX(ans.submitted_at)               AS last_answer,
    q.extraction_batch_id
FROM questions q
LEFT JOIN survey_sites ss  ON q.survey_site_id = ss.site_id
LEFT JOIN accounts a       ON q.account_id = a.account_id
LEFT JOIN answers ans      ON q.question_id = ans.question_id
GROUP BY q.question_id, q.survey_site_id, ss.site_name, q.account_id, a.username,
         q.question_text, q.question_type, q.question_category, q.survey_name,
         q.survey_complete, q.options, q.click_element, q.input_element,
         q.submit_element, q.required, q.extracted_at, q.is_active,
         q.used_in_workflow, q.used_at, q.extraction_batch_id;

COMMENT ON VIEW question_stats IS 'Questions with answer statistics, click elements, and survey tracking';

DROP VIEW IF EXISTS available_urls CASCADE;
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
JOIN accounts a      ON au.account_id = a.account_id
JOIN survey_sites ss ON au.site_id = ss.site_id
WHERE au.is_used = FALSE
ORDER BY au.is_default DESC, au.created_at DESC;

DROP VIEW IF EXISTS available_questions CASCADE;
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
    q.survey_name,
    q.options,
    q.click_element,
    q.input_element,
    q.submit_element,
    q.required,
    q.extracted_at
FROM questions q
JOIN accounts a      ON q.account_id = a.account_id
JOIN survey_sites ss ON q.survey_site_id = ss.site_id
WHERE q.used_in_workflow = FALSE AND q.is_active = TRUE
ORDER BY ss.site_name, q.survey_name, q.question_category, q.extracted_at DESC;

DROP VIEW IF EXISTS workflows_ready_for_upload CASCADE;
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
    q.survey_name,
    q.click_element
FROM workflows w
JOIN accounts a      ON w.account_id = a.account_id
JOIN survey_sites ss ON w.site_id = ss.site_id
LEFT JOIN questions q ON w.question_id = q.question_id
WHERE w.uploaded_to_chrome = FALSE AND w.is_active = TRUE
ORDER BY w.created_time DESC;

DROP VIEW IF EXISTS recent_extractions CASCADE;
CREATE VIEW recent_extractions AS
SELECT
    extraction_batch_id,
    COUNT(*)                                                     AS question_count,
    MIN(extracted_at)                                            AS first_extracted,
    MAX(extracted_at)                                            AS last_extracted,
    COUNT(DISTINCT survey_site_id)                               AS site_count,
    COUNT(DISTINCT account_id)                                   AS account_count,
    COUNT(DISTINCT question_type)                                AS type_count,
    COUNT(DISTINCT question_category)                            AS category_count,
    COUNT(DISTINCT survey_name)                                  AS survey_count,
    COUNT(CASE WHEN click_element IS NOT NULL THEN 1 END)       AS questions_with_click
FROM questions
WHERE extraction_batch_id IS NOT NULL
GROUP BY extraction_batch_id
ORDER BY last_extracted DESC;

DROP VIEW IF EXISTS prompt_backup_summary CASCADE;
CREATE VIEW prompt_backup_summary AS
SELECT
    p.prompt_id,
    p.account_id,
    a.username,
    p.name                      AS current_prompt_name,
    COUNT(pb.backup_id)         AS total_backups,
    MAX(pb.version_number)      AS latest_version,
    MIN(pb.backed_up_at)        AS first_backup,
    MAX(pb.backed_up_at)        AS latest_backup
FROM prompts p
LEFT JOIN accounts a        ON p.account_id = a.account_id
LEFT JOIN prompt_backups pb ON p.prompt_id = pb.prompt_id
GROUP BY p.prompt_id, p.account_id, a.username, p.name;

DROP VIEW IF EXISTS screening_summary CASCADE;
CREATE VIEW screening_summary AS
SELECT
    sr.account_id,
    a.username,
    sr.site_id,
    ss.site_name,
    sr.survey_name,
    COUNT(*)                                                              AS total_attempts,
    COUNT(*) FILTER (WHERE sr.status = 'passed')                        AS passed,
    COUNT(*) FILTER (WHERE sr.status = 'complete')                      AS complete,
    COUNT(*) FILTER (WHERE sr.status = 'failed')                        AS failed,
    COUNT(*) FILTER (WHERE sr.status = 'pending')                       AS pending,
    ROUND(
        COUNT(*) FILTER (WHERE sr.status IN ('passed','complete'))::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                                     AS pass_rate_pct,
    MAX(sr.started_at)                                                    AS last_attempt
FROM screening_results sr
JOIN accounts a      ON sr.account_id = a.account_id
JOIN survey_sites ss ON sr.site_id = ss.site_id
GROUP BY sr.account_id, a.username, sr.site_id, ss.site_name, sr.survey_name;

COMMENT ON VIEW screening_summary IS 'Pass/fail rates per account per survey';

DROP VIEW IF EXISTS proxy_config_summary CASCADE;
CREATE VIEW proxy_config_summary AS
SELECT
    pc.proxy_id,
    pc.account_id,
    a.username,
    pc.proxy_type,
    pc.host,
    pc.port,
    pc.is_active,
    pc.created_at,
    pc.updated_at,
    CASE WHEN a.active_proxy_id = pc.proxy_id THEN TRUE ELSE FALSE END AS is_active_for_account
FROM proxy_configs pc
JOIN accounts a ON pc.account_id = a.account_id
ORDER BY a.username, pc.created_at DESC;

COMMENT ON VIEW proxy_config_summary IS 'Proxy configurations per account with activation status';

-- ============= INITIAL DATA =============

INSERT INTO survey_sites (site_name, description)
SELECT * FROM (VALUES
    ('Top Surveys',         'General survey site with high-paying opportunities'),
    ('Quick Rewards',       'Fast surveys with instant payouts'),
    ('Survey Junkie',       'Popular survey platform'),
    ('Pinecone Research',   'Product testing and surveys'),
    ('Swagbucks',           'Earn points for surveys and activities'),
    ('InboxDollars',        'Paid surveys and offers'),
    ('MyPoints',            'Surveys and shopping rewards'),
    ('Toluna',              'Community-based surveys'),
    ('YouGov',              'Opinion surveys on current events'),
    ('Vindale Research',    'High-paying survey site'),
    ('PrizeRebel',          'Surveys and offers platform'),
    ('LifePoints',          'Mobile and web surveys'),
    ('SurveyMonkey Rewards','Paid survey platform'),
    ('OnePoll',             'Daily news and opinion surveys'),
    ('Valued Opinions',     'Consumer opinion surveys')
) AS v(site_name, description)
WHERE NOT EXISTS (SELECT 1 FROM survey_sites LIMIT 1);

-- ============= SUCCESS MESSAGE =============
SELECT '✅ Survey automation schema initialized successfully!' AS status;
SELECT 'Tables: accounts, proxy_configs, account_cookies, survey_sites, account_urls, prompts, prompt_backups, workflows, questions, answers, extraction_state, workflow_generation_log, screening_results' AS tables;
SELECT 'Views: survey_site_summary, account_summary, question_stats, available_urls, available_questions, workflows_ready_for_upload, recent_extractions, prompt_backup_summary, screening_summary, proxy_config_summary' AS views;
SELECT 'Functions: get_questions_by_site, get_unused_questions, get_answers_for_question, get_prompt_for_account, get_workflows_by_site, record_extraction_batch, get_answer_statistics' AS functions;
SELECT '🎯 Schema optimized for SURVEY AUTOMATION with browser-use cloud SDK + proxy support + screening result tracking' AS feature;