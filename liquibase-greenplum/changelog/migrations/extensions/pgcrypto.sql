--liquibase formatted sql

--changeset codex:pgcrypto
CREATE EXTENSION IF NOT EXISTS pgcrypto;
