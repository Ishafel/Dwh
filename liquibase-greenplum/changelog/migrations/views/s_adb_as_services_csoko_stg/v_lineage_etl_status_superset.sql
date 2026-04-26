--liquibase formatted sql

--changeset codex:v_lineage_etl_status_superset splitStatements:false
DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_lineage_etl_status_superset;

CREATE VIEW s_adb_as_services_csoko_stg.v_lineage_etl_status_superset AS
WITH per_table AS (
    SELECT
        tgt_table AS root_view_name,
        max(finished_at) AS last_event_at,
        max(CASE WHEN status = 'ERROR' THEN finished_at END) AS last_failed_at
      FROM s_adb_as_services_csoko_stg.etl_run
     WHERE tgt_table IS NOT NULL
     GROUP BY tgt_table
),
last_status AS (
    SELECT
        e.tgt_table AS root_view_name,
        e.status,
        row_number() OVER (
            PARTITION BY e.tgt_table
            ORDER BY e.finished_at DESC, e.started_at DESC
        ) AS rn
      FROM s_adb_as_services_csoko_stg.etl_run e
     WHERE e.tgt_table IS NOT NULL
)
SELECT
    p.root_view_name,
    p.last_event_at::timestamp AS last_event_at,
    p.last_failed_at::timestamp AS last_failed_at,
    CASE
        WHEN s.status = 'ERROR' THEN 'red'
        WHEN s.status = 'SUCCESS' THEN 'green'
        ELSE 'unknown'
    END AS mart_status
  FROM per_table p
  LEFT JOIN last_status s
    ON s.root_view_name = p.root_view_name
   AND s.rn = 1;
