-- Validation check table

CREATE TABLE IF NOT EXISTS validation_audit_log (
    id SERIAL PRIMARY KEY,
    check_name TEXT,
    status TEXT CHECK (status IN ('PASS', 'FAIL')),
    failure_count INT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);
