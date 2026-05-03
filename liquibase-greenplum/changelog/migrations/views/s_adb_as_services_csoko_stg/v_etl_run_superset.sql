--liquibase formatted sql

--changeset codex:v_etl_run_superset runOnChange:true runAlways:true splitStatements:false
DROP VIEW IF EXISTS s_adb_as_services_csoko_stg.v_etl_run_superset CASCADE;

CREATE OR REPLACE VIEW s_adb_as_services_csoko_stg.v_etl_run_superset
AS SELECT etl_run.run_id,
          etl_run.function_name,
          etl_run.env,
          etl_run.src_schema,
          etl_run.src_table,
          etl_run.tgt_schema,
          etl_run.tgt_table,
          etl_run.do_truncate,
          etl_run.do_analyze,
          etl_run.status,
          etl_run.rows_inserted,
          etl_run.error_text,
          etl_run.started_at,
          etl_run.finished_at,
          COALESCE(etl_run.finished_at, clock_timestamp()) AS finished_at_eff,
          COALESCE(etl_run.duration_ms, (date_part('epoch'::text, COALESCE(etl_run.finished_at, clock_timestamp()) - etl_run.started_at) * 1000::double precision)::bigint) AS duration_ms_eff,
          COALESCE(etl_run.duration_ms, (date_part('epoch'::text, COALESCE(etl_run.finished_at, clock_timestamp()) - etl_run.started_at) * 1000::double precision)::bigint)::numeric / 1000.0 AS duration_sec,
          date_trunc('minute'::text, etl_run.started_at) AS started_minute,
          date_trunc('hour'::text, etl_run.started_at) AS started_hour,
          date_trunc('day'::text, etl_run.started_at) AS started_day,
          date_part('hour'::text, etl_run.started_at)::integer AS etl_hour,
          date_part('dow'::text, etl_run.started_at)::integer AS etl_dow,
          date_part('isodow'::text, etl_run.started_at)::integer AS etl_isodow,
          to_char(etl_run.started_at, 'Dy'::text) AS etl_dow_name,
          CASE
              WHEN etl_run.error_text IS NULL OR btrim(etl_run.error_text) = ''::text THEN NULL::text
              ELSE "left"(regexp_replace(regexp_replace(etl_run.error_text, '\s+'::text, ' '::text, 'g'::text), '0x[0-9A-Fa-f]+|\b\d+\b'::text, '?'::text, 'g'::text), 160)
          END AS error_key,
          CASE
                        WHEN etl_run.rows_inserted IS NULL OR etl_run.rows_inserted < 0 THEN 0::numeric
                        WHEN COALESCE(etl_run.duration_ms, (date_part('epoch'::text, COALESCE(etl_run.finished_at, clock_timestamp()) - etl_run.started_at) * 1000::double precision)::bigint) <= 0 THEN 0::numeric
                        WHEN etl_run.rows_inserted = 0 THEN 0
                        ELSE etl_run.rows_inserted::numeric / (COALESCE(etl_run.duration_ms, (date_part('epoch'::text, COALESCE(etl_run.finished_at, clock_timestamp()) - etl_run.started_at) * 1000::double precision)::bigint)::numeric / 1000.0)
          END AS rows_per_sec,
          etl_run.db_name,
          etl_run.username,
          etl_run.session_pid,
          etl_run.txid,
          etl_run.extra,
          CASE
                        WHEN etl_run.status = 'ERROR'::text THEN 1
                        ELSE 0
                    END AS is_failed,
          CASE
                        WHEN etl_run.status = 'SUCCESS'::text THEN 1
                        ELSE 0
                    END AS is_success,
          CASE
              WHEN date_trunc('day', etl_run.started_at) = date_trunc('day', CURRENT_DATE) THEN 1
              ELSE 0
          END AS is_today_run,
          etl_run.src_row_count,
          etl_run.tgt_row_count
FROM s_adb_as_services_csoko_stg.etl_run;

COMMENT ON VIEW s_adb_as_services_csoko_stg.v_etl_run_superset IS 'Витрина для Apache Superset: логи запусков ETL (etl_run) с вычисленными полями для визуализации (effective finished_at, длительность, зерно времени, час/день недели, нормализованный ключ ошибки, скорость rows/sec, флаги статусов).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.run_id IS 'UUID запуска ETL (ключ записи логирования).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.function_name IS 'Имя функции/процесса ETL, который выполнил загрузку.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.env IS 'Окружение выполнения (DEV/TEST/PROD и т.п.).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.src_schema IS 'Исходная схема (источник данных).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.src_table IS 'Исходная таблица (источник данных).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.tgt_schema IS 'Целевая схема (приёмник данных).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.tgt_table IS 'Целевая таблица (приёмник данных).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.do_truncate IS 'Флаг: выполнялась ли очистка целевой таблицы перед загрузкой (TRUNCATE).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.do_analyze IS 'Флаг: выполнялся ли ANALYZE после загрузки.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.status IS 'Статус выполнения запуска: SUCCESS / ERROR.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.rows_inserted IS 'Количество вставленных строк (если процесс пишет эту метрику).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.error_text IS 'Текст ошибки при ERROR (NULL при успешном выполнении).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.started_at IS 'Время начала запуска ETL.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.finished_at IS 'Время завершения запуска (NULL, если запуск ещё выполняется или не записал завершение).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.finished_at_eff IS 'Эффективное время завершения: finished_at, а если NULL — текущее время (clock_timestamp()) для расчёта длительности RUNNING.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.duration_ms_eff IS 'Эффективная длительность в миллисекундах: duration_ms из логов, а если NULL — вычисляется как (finished_at_eff - started_at).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.duration_sec IS 'Эффективная длительность в секундах (duration_ms_eff / 1000).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.started_minute IS 'started_at, округлённое до минуты (удобно для агрегации во временных рядах).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.started_hour IS 'started_at, округлённое до часа (удобно для агрегации во временных рядах).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.started_day IS 'started_at, округлённое до дня (удобно для агрегации по дням/календарю).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.etl_hour IS 'Час запуска (0..23) из started_at. Удобно для heatmap и анализа нагрузки по времени суток.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.etl_dow IS 'День недели запуска (0..6) из started_at. В PostgreSQL: 0=воскресенье, 1=понедельник, ..., 6=суббота.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.etl_isodow IS 'День недели запуска по ISO (1..7): 1=понедельник, ..., 7=воскресенье. Удобно для привычной сортировки.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.etl_dow_name IS 'Короткое имя дня недели (формат Dy). Зависит от настройки lc_time на сервере БД.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.error_key IS 'Нормализованный ключ ошибки: error_text с схлопнутыми пробелами и заменой чисел/hex на «?» + обрезка до 160 символов. Полезно для топа типовых ошибок.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.rows_per_sec IS 'Скорость загрузки (строк/сек): rows_inserted / duration_sec. NULL если rows_inserted отсутствует или duration некорректна.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.db_name IS 'Имя базы данных, в которой выполнялся запуск.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.username IS 'Пользователь БД, от имени которого выполнялся запуск.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.session_pid IS 'PID серверного процесса сессии в момент запуска (для диагностики).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.txid IS 'Идентификатор транзакции (txid_current) в момент логирования запуска.';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.extra IS 'Дополнительная информация процесса (рекомендуется хранить структурировано, напр. JSON-строкой).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.is_failed IS 'Флаг 1/0: запуск завершился ошибкой (status = ERROR).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.is_success IS 'Флаг 1/0: запуск завершился успешно (status = SUCCESS).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.is_today_run IS 'Флаг 1/0: запуск начался сегодня (по полю started_at, без учёта часового пояса).';
COMMENT ON COLUMN s_adb_as_services_csoko_stg.v_etl_run_superset.src_row_count IS 'Количество строк в таблице источнике.';
COMMENT on column s_adb_as_services_csoko_stg.v_etl_run_superset.tgt_row_count IS 'Количество строк в целевой таблице.';
