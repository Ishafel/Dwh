--liquibase formatted sql

--changeset codex:f_refresh_table_v2 runOnChange:true splitStatements:false
DROP FUNCTION IF EXISTS s_adb_as_services_csoko_stg.f_refresh_table_v2(text, text, text, text, bool, bool);
DROP FUNCTION IF EXISTS s_adb_as_services_csoko_stg.f_refresh_table_v2(text, text, text, text, bool, bool, bool);

CREATE OR REPLACE FUNCTION s_adb_as_services_csoko_stg.f_refresh_table_v2(
    p_schema_target text,
    p_table_target text,
    p_schema_src text,
    p_table_src text,
    p_truncate boolean DEFAULT true,
    p_do_analyze boolean DEFAULT true,
    p_compare_row_count boolean DEFAULT false
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    v_rows        bigint := 0;
    v_cols        text;
    v_sql         text;
    v_tmp_table   text;

    v_run_start   timestamptz := clock_timestamp();
    v_finished_at timestamptz;
    v_duration_ms bigint;

    v_is_error boolean := false;
    v_error    text := '';
    v_extra    text := '';

    v_src_row_count bigint := 0;
    v_tgt_row_count bigint := 0;

    -- effective refresh mode
    v_effective_truncate boolean := p_truncate;
    v_has_ctl_loading boolean := false;
    v_missing_ctl_loading_count bigint := 0;
    v_src_ctl_loading_col text;
    v_tgt_ctl_loading_col text;
    v_after_destructive_step boolean := false;

    -- diagnostics
    v_sqlstate text := '';
    v_msg      text := '';
    v_detail   text := '';
    v_hint     text := '';
    v_context  text := '';

    -- mart status
    v_last_event_at timestamp;
    v_last_failed_at timestamp;
    v_mart_status text;
BEGIN
    /* 1) Validation */
    IF p_schema_target IS NULL OR p_table_target IS NULL THEN
        v_is_error := true;
        v_rows := -1;
        v_error := format('Target table is not specified (schema=%s, table=%s)', p_schema_target, p_table_target);
    ELSIF p_schema_src IS NULL OR p_table_src IS NULL THEN
        v_is_error := true;
        v_rows := -1;
        v_error := format('Source table is not specified (schema=%s, table=%s)', p_schema_src, p_table_src);
    END IF;

    /* 2) Columns: all source columns in source order */
    IF NOT v_is_error THEN
        SELECT string_agg(quote_ident(s.column_name), ', ' ORDER BY s.ordinal_position)
          INTO v_cols
          FROM information_schema.columns s
         WHERE s.table_schema = p_schema_src
           AND s.table_name   = p_table_src;

        IF v_cols IS NULL OR length(btrim(v_cols)) = 0 THEN
            v_is_error := true;
            v_rows := -1;
            v_error := format('Source table %I.%I not found or has no columns', p_schema_src, p_table_src);
        END IF;
    END IF;

    /*
      2.1) ctl_loading set check.

      If target already contains ctl_loading values missing from the
      external source table, force a full refresh.
    */
    IF NOT v_is_error THEN
        SELECT s.column_name
          INTO v_src_ctl_loading_col
          FROM information_schema.columns s
         WHERE s.table_schema = p_schema_src
           AND s.table_name   = p_table_src
           AND lower(s.column_name) = 'ctl_loading'
         ORDER BY s.ordinal_position
         LIMIT 1;

        SELECT t.column_name
          INTO v_tgt_ctl_loading_col
          FROM information_schema.columns t
         WHERE t.table_schema = p_schema_target
           AND t.table_name   = p_table_target
           AND lower(t.column_name) = 'ctl_loading'
         ORDER BY t.ordinal_position
         LIMIT 1;

        v_has_ctl_loading := v_src_ctl_loading_col IS NOT NULL
                             AND v_tgt_ctl_loading_col IS NOT NULL;

        IF v_has_ctl_loading THEN
            v_sql := format(
                'SELECT COUNT(*)
                   FROM (
                        SELECT DISTINCT %1$I AS ctl_loading
                          FROM %2$I.%3$I
                         WHERE %1$I IS NOT NULL
                   ) t
                  WHERE NOT EXISTS (
                        SELECT 1
                          FROM (
                               SELECT DISTINCT %4$I AS ctl_loading
                                 FROM %5$I.%6$I
                                WHERE %4$I IS NOT NULL
                          ) s
                         WHERE s.ctl_loading IS NOT DISTINCT FROM t.ctl_loading
                  )',
                v_tgt_ctl_loading_col,
                p_schema_target,
                p_table_target,
                v_src_ctl_loading_col,
                p_schema_src,
                p_table_src
            );

            v_extra := v_extra || format('missing_ctl_loading_check_sql=%s; ', v_sql);

            BEGIN
                EXECUTE v_sql INTO v_missing_ctl_loading_count;

                IF v_missing_ctl_loading_count > 0 THEN
                    v_effective_truncate := true;
                    v_extra := v_extra || format(
                        'force_truncate_by_missing_ctl_loading=true; missing_ctl_loading_count=%s; ',
                        v_missing_ctl_loading_count
                    );
                ELSE
                    v_extra := v_extra || 'force_truncate_by_missing_ctl_loading=false; missing_ctl_loading_count=0; ';
                END IF;
            EXCEPTION WHEN OTHERS THEN
                GET STACKED DIAGNOSTICS
                    v_sqlstate = RETURNED_SQLSTATE,
                    v_msg      = MESSAGE_TEXT,
                    v_detail   = PG_EXCEPTION_DETAIL,
                    v_hint     = PG_EXCEPTION_HINT,
                    v_context  = PG_EXCEPTION_CONTEXT;

                v_is_error := true;
                v_rows := -1;
                v_error :=
                    'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                    '; MESSAGE=' || coalesce(v_msg,'') ||
                    CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                    CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                    CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

                v_extra := v_extra || format('failed_sql=%s; ', v_sql);
            END;
        ELSE
            v_extra := v_extra || format(
                'ctl_loading_check_skipped=true; source_ctl_loading_col=%s; target_ctl_loading_col=%s; ',
                coalesce(v_src_ctl_loading_col, 'null'),
                coalesce(v_tgt_ctl_loading_col, 'null')
            );
        END IF;
    END IF;

    /*
      2.2) Mart Lineage stop check.
    */
    IF NOT v_is_error THEN
        SELECT last_event_at, last_failed_at, mart_status
          INTO v_last_event_at, v_last_failed_at, v_mart_status
          FROM s_adb_as_services_csoko_stg.v_lineage_etl_status_superset
         WHERE root_view_name = p_table_target;

        IF v_last_failed_at = v_last_event_at THEN
            RAISE EXCEPTION 'Ошибка: последнее событие завершилось с ошибкой (last_failed_at = last_event_at = %)', v_last_event_at;
        END IF;

        IF DATE(v_last_event_at) != CURRENT_DATE THEN
            RAISE EXCEPTION 'Ошибка: данные не обновлены сегодня. Последнее обновление: %', v_last_event_at;
        END IF;

        IF lower(v_mart_status) = lower('red') THEN
            RAISE EXCEPTION 'Ошибка: статус витрины "red" (критическая ошибка)';
        END IF;
    END IF;

    /*
      3) For full refresh, load source data into a temp table first.

      The target table is not touched until source rows have been validated
      against the target column types.
    */
    IF NOT v_is_error AND v_effective_truncate THEN
        BEGIN
            v_tmp_table := format(
                'tmp_f_refresh_table_v2_%s',
                replace(gen_random_uuid()::text, '-', '_')
            );

            v_sql := format(
                'CREATE TEMP TABLE %I (LIKE %I.%I INCLUDING DEFAULTS) ON COMMIT DROP DISTRIBUTED RANDOMLY',
                v_tmp_table,
                p_schema_target,
                p_table_target
            );
            v_extra := v_extra || format('create_temp_sql=%s; ', v_sql);
            EXECUTE v_sql;

            v_sql := format(
                'INSERT INTO %I (%s) SELECT %s FROM %I.%I',
                v_tmp_table,
                v_cols,
                v_cols,
                p_schema_src,
                p_table_src
            );
            v_extra := v_extra || format('insert_temp_sql=%s; ', v_sql);
            EXECUTE v_sql;
            GET DIAGNOSTICS v_rows = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_is_error := true;
            v_rows := -1;
            v_error :=
                'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                '; MESSAGE=' || coalesce(v_msg,'') ||
                CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

            v_extra := v_extra || format('failed_sql=%s; ', v_sql);
        END;
    END IF;

    /* 4) Write rows into target */
    IF NOT v_is_error AND v_effective_truncate THEN
        BEGIN
            v_sql := format('TRUNCATE TABLE %I.%I', p_schema_target, p_table_target);
            v_extra := v_extra || format('truncate_sql=%s; ', v_sql);
            EXECUTE v_sql;
            v_after_destructive_step := true;

            v_sql := format(
                'INSERT INTO %I.%I (%s) SELECT %s FROM %I',
                p_schema_target,
                p_table_target,
                v_cols,
                v_cols,
                v_tmp_table
            );
            v_extra := v_extra || format('insert_sql=%s; ', v_sql);
            EXECUTE v_sql;
            GET DIAGNOSTICS v_rows = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_error :=
                'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                '; MESSAGE=' || coalesce(v_msg,'') ||
                CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

            v_extra := v_extra || format('failed_sql=%s; ', v_sql);

            IF v_after_destructive_step THEN
                RAISE;
            END IF;

            v_is_error := true;
            v_rows := -1;
        END;
    END IF;

    IF NOT v_is_error AND NOT v_effective_truncate THEN
        v_sql := format(
            'INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
            p_schema_target, p_table_target, v_cols,
            v_cols,
            p_schema_src, p_table_src
        );
        v_extra := v_extra || format('insert_sql=%s; ', v_sql);

        BEGIN
            EXECUTE v_sql;
            GET DIAGNOSTICS v_rows = ROW_COUNT;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_is_error := true;
            v_rows := -1;
            v_error :=
                'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                '; MESSAGE=' || coalesce(v_msg,'') ||
                CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

            v_extra := v_extra || format('failed_sql=%s; ', v_sql);
        END;
    END IF;

    /* 5) ANALYZE */
    IF NOT v_is_error AND p_do_analyze THEN
        BEGIN
            v_sql := format('ANALYZE %I.%I', p_schema_target, p_table_target);
            v_extra := v_extra || format('analyze_sql=%s; ', v_sql);
            EXECUTE v_sql;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_is_error := true;
            v_rows := -1;
            v_error :=
                'SQLSTATE=' || coalesce(v_sqlstate,'') ||
                '; MESSAGE=' || coalesce(v_msg,'') ||
                CASE WHEN v_detail  IS NOT NULL THEN '; DETAIL='  || v_detail  ELSE '' END ||
                CASE WHEN v_hint    IS NOT NULL THEN '; HINT='    || v_hint    ELSE '' END ||
                CASE WHEN v_context IS NOT NULL THEN '; CONTEXT=' || v_context ELSE '' END;

            v_extra := v_extra || format('failed_sql=%s; ', v_sql);

            IF v_after_destructive_step THEN
                RAISE;
            END IF;
        END;
    END IF;

    /* 6) Count source rows and actual target rows */
    IF NOT v_is_error THEN
        BEGIN
            v_sql := format('SELECT COUNT(*) FROM %I.%I', p_schema_src, p_table_src);
            EXECUTE v_sql INTO v_src_row_count;

            v_sql := format('SELECT COUNT(*) FROM %I.%I', p_schema_target, p_table_target);
            EXECUTE v_sql INTO v_tgt_row_count;

            v_extra := v_extra || format(
                'source_row_count=%s; target_row_count=%s; effective_truncate=%s; input_truncate=%s; ',
                v_src_row_count,
                v_tgt_row_count,
                v_effective_truncate,
                p_truncate
            );

            IF p_compare_row_count AND v_src_row_count != v_tgt_row_count THEN
                v_is_error := true;
                v_error := format('Row count mismatch: source=%s, target=%s', v_src_row_count, v_tgt_row_count);
                v_rows := -1;

                IF v_after_destructive_step THEN
                    RAISE EXCEPTION '%', v_error;
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_sqlstate = RETURNED_SQLSTATE,
                v_msg      = MESSAGE_TEXT,
                v_detail   = PG_EXCEPTION_DETAIL,
                v_hint     = PG_EXCEPTION_HINT,
                v_context  = PG_EXCEPTION_CONTEXT;

            v_extra := v_extra || format(
                'row_count_query_error=SQLSTATE=%s; MESSAGE=%s; failed_sql=%s; ',
                coalesce(v_sqlstate,''),
                coalesce(v_msg,''),
                v_sql
            );

            IF v_after_destructive_step AND p_compare_row_count THEN
                RAISE;
            END IF;
        END;
    END IF;

    /* 7) Final journal insert */
    v_finished_at := clock_timestamp();
    v_duration_ms := (extract(epoch from (v_finished_at - v_run_start)) * 1000)::bigint;

    BEGIN
        INSERT INTO s_adb_as_services_csoko_stg.etl_run(
            run_id,
            function_name,
            src_schema, src_table,
            tgt_schema, tgt_table,
            do_truncate, do_analyze,
            status,
            rows_inserted,
            error_text,
            started_at,
            finished_at,
            duration_ms,
            extra,
            src_row_count,
            tgt_row_count
        )
        VALUES (
            gen_random_uuid(),
            's_adb_as_services_csoko_stg.f_refresh_table_v2(text, text, text, text, bool, bool, bool)',
            p_schema_src, p_table_src,
            p_schema_target, p_table_target,
            v_effective_truncate, p_do_analyze,
            CASE WHEN v_is_error THEN 'ERROR' ELSE 'SUCCESS' END,
            v_rows,
            CASE WHEN v_is_error THEN v_error ELSE NULL END,
            v_run_start,
            v_finished_at,
            v_duration_ms,
            v_extra,
            v_src_row_count,
            v_tgt_row_count
        );
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    IF v_is_error THEN
        RAISE INFO 'Error in f_refresh_table_v2: %', v_error;
    END IF;

    RETURN v_rows;
END;
$function$;
