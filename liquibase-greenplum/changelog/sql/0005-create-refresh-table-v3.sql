DROP FUNCTION IF EXISTS s_adb_as_services_csoko_stg.f_refresh_table_v3(text, text, text, text, text, bool, bool, bool);

CREATE OR REPLACE FUNCTION s_adb_as_services_csoko_stg.f_refresh_table_v3(
    p_schema_target text,
    p_table_target text,
    p_schema_src text,
    p_object_src text,
    p_source_kind text DEFAULT 'table',
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
    v_target_cols text;
    v_sql         text;
    v_source_from text;
    v_source_kind text;
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
    v_source_function_exists boolean := false;

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
    v_source_kind := lower(coalesce(nullif(btrim(p_source_kind), ''), 'table'));

    IF v_source_kind IN ('relation', 'view', 'external_table') THEN
        v_source_kind := 'table';
    ELSIF v_source_kind IN ('setof', 'srf') THEN
        v_source_kind := 'function';
    END IF;

    IF p_schema_target IS NULL OR p_table_target IS NULL THEN
        v_is_error := true;
        v_rows := -1;
        v_error := format('Target table is not specified (schema=%s, table=%s)', p_schema_target, p_table_target);
    ELSIF p_schema_src IS NULL OR p_object_src IS NULL THEN
        v_is_error := true;
        v_rows := -1;
        v_error := format('Source object is not specified (schema=%s, object=%s)', p_schema_src, p_object_src);
    ELSIF v_source_kind NOT IN ('table', 'function') THEN
        v_is_error := true;
        v_rows := -1;
        v_error := format('Unsupported source_kind=%s. Use table or function', p_source_kind);
    END IF;

    /* 2) Target columns are required for temp-table loading and function sources. */
    IF NOT v_is_error THEN
        SELECT string_agg(quote_ident(t.column_name), ', ' ORDER BY t.ordinal_position)
          INTO v_target_cols
          FROM information_schema.columns t
         WHERE t.table_schema = p_schema_target
           AND t.table_name   = p_table_target;

        IF v_target_cols IS NULL OR length(btrim(v_target_cols)) = 0 THEN
            v_is_error := true;
            v_rows := -1;
            v_error := format('Target table %I.%I not found or has no columns', p_schema_target, p_table_target);
        END IF;
    END IF;

    /* 3) Build source relation expression and refresh column list. */
    IF NOT v_is_error THEN
        IF v_source_kind = 'table' THEN
            v_source_from := format('%I.%I', p_schema_src, p_object_src);

            SELECT string_agg(quote_ident(s.column_name), ', ' ORDER BY s.ordinal_position)
              INTO v_cols
              FROM information_schema.columns s
             WHERE s.table_schema = p_schema_src
               AND s.table_name   = p_object_src;

            IF v_cols IS NULL OR length(btrim(v_cols)) = 0 THEN
                v_is_error := true;
                v_rows := -1;
                v_error := format('Source table %I.%I not found or has no columns', p_schema_src, p_object_src);
            END IF;
        ELSE
            v_source_from := format('%I.%I()', p_schema_src, p_object_src);
            v_cols := v_target_cols;

            SELECT EXISTS (
                SELECT 1
                  FROM pg_proc p
                  JOIN pg_namespace n
                    ON n.oid = p.pronamespace
                 WHERE n.nspname = p_schema_src
                   AND p.proname = p_object_src
                   AND p.pronargs = 0
                   AND p.proretset
                   AND (
                       p.prorettype <> 'record'::regtype
                       OR EXISTS (
                           SELECT 1
                             FROM unnest(p.proargmodes) AS m(mode)
                            WHERE m.mode::text IN ('o', 'b', 't')
                       )
                   )
            )
              INTO v_source_function_exists;

            IF NOT v_source_function_exists THEN
                v_is_error := true;
                v_rows := -1;
                v_error := format(
                    'Source function %I.%I() not found or does not return SETOF rows with named columns',
                    p_schema_src,
                    p_object_src
                );
            END IF;
        END IF;

        v_extra := v_extra || format('source_kind=%s; source_from=%s; ', v_source_kind, v_source_from);
    END IF;

    /*
      3.1) ctl_loading set check.

      For function sources the check is skipped deliberately: calling a set-returning
      function just to inspect ctl_loading can be expensive and non-idempotent.
    */
    IF NOT v_is_error THEN
        SELECT t.column_name
          INTO v_tgt_ctl_loading_col
          FROM information_schema.columns t
         WHERE t.table_schema = p_schema_target
           AND t.table_name   = p_table_target
           AND lower(t.column_name) = 'ctl_loading'
         ORDER BY t.ordinal_position
         LIMIT 1;

        IF v_source_kind = 'table' THEN
            SELECT s.column_name
              INTO v_src_ctl_loading_col
              FROM information_schema.columns s
             WHERE s.table_schema = p_schema_src
               AND s.table_name   = p_object_src
               AND lower(s.column_name) = 'ctl_loading'
             ORDER BY s.ordinal_position
             LIMIT 1;
        END IF;

        v_has_ctl_loading := v_source_kind = 'table'
                             AND v_src_ctl_loading_col IS NOT NULL
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
                p_object_src
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
      3.2) Mart Lineage stop check.
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
      4) For full refresh, load source data into a temp table first.

      The target table is not touched until source rows have been validated
      against the target column types.
    */
    IF NOT v_is_error AND v_effective_truncate THEN
        BEGIN
            v_tmp_table := format(
                'tmp_f_refresh_table_v3_%s',
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
                'INSERT INTO %I (%s) SELECT %s FROM %s',
                v_tmp_table,
                v_cols,
                v_cols,
                v_source_from
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

    /* 5) Write rows into target */
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
            'INSERT INTO %I.%I (%s) SELECT %s FROM %s',
            p_schema_target, p_table_target, v_cols,
            v_cols,
            v_source_from
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

    /* 6) ANALYZE */
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

    /* 7) Count rows. Set-returning functions are not called a second time. */
    IF NOT v_is_error THEN
        BEGIN
            v_src_row_count := v_rows;

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

    /* 8) Final journal insert */
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
            's_adb_as_services_csoko_stg.f_refresh_table_v3(text, text, text, text, text, bool, bool, bool)',
            p_schema_src, p_object_src,
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
        RAISE INFO 'Error in f_refresh_table_v3: %', v_error;
    END IF;

    RETURN v_rows;
END;
$function$;
