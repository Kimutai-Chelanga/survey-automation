-- =====================================================
-- Migration: Add batch_logs and batch_screenshots tables
-- Run on fresh database or existing one
-- =====================================================

-- Table: batch_logs
CREATE TABLE IF NOT EXISTS batch_logs (
    log_id     SERIAL PRIMARY KEY,
    batch_id   VARCHAR(100) NOT NULL,
    account_id INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
    site_id    INTEGER REFERENCES survey_sites(site_id) ON DELETE CASCADE,
    log_level  VARCHAR(20)  NOT NULL DEFAULT 'INFO',
    message    TEXT         NOT NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table: batch_screenshots
CREATE TABLE IF NOT EXISTS batch_screenshots (
    screenshot_id SERIAL PRIMARY KEY,
    batch_id      VARCHAR(100) NOT NULL,
    account_id    INTEGER REFERENCES accounts(account_id) ON DELETE CASCADE,
    site_id       INTEGER REFERENCES survey_sites(site_id) ON DELETE CASCADE,
    survey_num    INTEGER      NOT NULL DEFAULT 0,
    stage         VARCHAR(100) NOT NULL,
    label         VARCHAR(255) NOT NULL,
    file_path     TEXT         NOT NULL,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast filtering
CREATE INDEX IF NOT EXISTS idx_batch_logs_batch    ON batch_logs(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_logs_account  ON batch_logs(account_id);
CREATE INDEX IF NOT EXISTS idx_batch_logs_site     ON batch_logs(site_id);
CREATE INDEX IF NOT EXISTS idx_batch_logs_created  ON batch_logs(created_at);

CREATE INDEX IF NOT EXISTS idx_batch_screenshots_batch   ON batch_screenshots(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_screenshots_account ON batch_screenshots(account_id);
CREATE INDEX IF NOT EXISTS idx_batch_screenshots_site    ON batch_screenshots(site_id);
CREATE INDEX IF NOT EXISTS idx_batch_screenshots_stage   ON batch_screenshots(stage);