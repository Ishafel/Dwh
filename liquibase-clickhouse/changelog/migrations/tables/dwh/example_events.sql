--liquibase formatted sql

--changeset codex:example_events splitStatements:false
CREATE TABLE IF NOT EXISTS example_events (
    event_id UInt64,
    event_name String,
    event_time DateTime
)
ENGINE = MergeTree
ORDER BY (event_time, event_id);

--rollback DROP TABLE IF EXISTS example_events;
