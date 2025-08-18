DO $do$
DECLARE
  col_f0 text; col_f1 text; col_time text; typ_time text; dyn_sql text;
BEGIN
  SELECT column_name INTO col_f0
    FROM information_schema.columns
   WHERE table_name='pool_hour_data'
     AND (column_name IN ('f0','fee0','fees0','fee_token0','fees_token0','token0_fee','token0_fees','collected_fee0','collected_fees0')
       OR column_name LIKE 'fee0%' OR column_name LIKE 'fees0%'
       OR column_name LIKE 'fee_token0%' OR column_name LIKE 'fees_token0%'
       OR column_name LIKE 'token0_fee%' OR column_name LIKE 'token0_fees%')
   ORDER BY ordinal_position LIMIT 1;

  SELECT column_name INTO col_f1
    FROM information_schema.columns
   WHERE table_name='pool_hour_data'
     AND (column_name IN ('f1','fee1','fees1','fee_token1','fees_token1','token1_fee','token1_fees','collected_fee1','collected_fees1')
       OR column_name LIKE 'fee1%' OR column_name LIKE 'fees1%'
       OR column_name LIKE 'fee_token1%' OR column_name LIKE 'fees_token1%'
       OR column_name LIKE 'token1_fee%' OR column_name LIKE 'token1_fees%')
   ORDER BY ordinal_position LIMIT 1;

  SELECT column_name, data_type INTO col_time, typ_time
    FROM information_schema.columns
   WHERE table_name='pool_hour_data'
     AND column_name IN ('hour_start_unix','hour_start','hour_ts','ts','timestamp','ts_unix','started_at')
   ORDER BY ordinal_position LIMIT 1;

  IF col_f0 IS NULL OR col_f1 IS NULL OR col_time IS NULL THEN
    RAISE EXCEPTION 'Required columns not found in pool_hour_data (fee0: %, fee1: %, time: %)', col_f0, col_f1, col_time;
  END IF;

  dyn_sql :=
    'drop view if exists v_pool_hour_fees_usd_partial; ' ||
    'create view v_pool_hour_fees_usd_partial as ' ||
    'select h.pool_id, ' ||
    CASE WHEN typ_time IN ('integer','bigint','numeric') THEN 'to_timestamp(h.'||col_time||')'
         ELSE 'h.'||quote_ident(col_time) END ||
    ' as hour_start_unix, ' ||
    'case when rs0.is_stable then h.'||quote_ident(col_f0)||' '||
         'when rs1.is_stable then h.'||quote_ident(col_f1)||' '||
         'else null end as fees_usd '||
    'from pool_hour_data h '||
    'join pools p on p.id=h.pool_id '||
    'join tokens t0 on t0.id=p.token0_id '||
    'join tokens t1 on t1.id=p.token1_id '||
    'left join ref_tokens rs0 on upper(t0.symbol)=rs0.symbol '||
    'left join ref_tokens rs1 on upper(t1.symbol)=rs1.symbol '||
    'where p.chain_id=1';
  EXECUTE dyn_sql;
END
$do$;

DO $do$
DECLARE
  col_f0 text; col_f1 text; col_date text; typ_date text; date_expr text; dyn_sql text;
BEGIN
  SELECT column_name INTO col_f0
    FROM information_schema.columns
   WHERE table_name='pool_day_data'
     AND (column_name IN ('f0','fee0','fees0','fee_token0','fees_token0','token0_fee','token0_fees','collected_fee0','collected_fees0')
       OR column_name LIKE 'fee0%' OR column_name LIKE 'fees0%'
       OR column_name LIKE 'fee_token0%' OR column_name LIKE 'fees_token0%'
       OR column_name LIKE 'token0_fee%' OR column_name LIKE 'token0_fees%')
   ORDER BY ordinal_position LIMIT 1;

  SELECT column_name INTO col_f1
    FROM information_schema.columns
   WHERE table_name='pool_day_data'
     AND (column_name IN ('f1','fee1','fees1','fee_token1','fees_token1','token1_fee','token1_fees','collected_fee1','collected_fees1')
       OR column_name LIKE 'fee1%' OR column_name LIKE 'fees1%'
       OR column_name LIKE 'fee_token1%' OR column_name LIKE 'fees_token1%'
       OR column_name LIKE 'token1_fee%' OR column_name LIKE 'token1_fees%')
   ORDER BY ordinal_position LIMIT 1;

  SELECT column_name, data_type INTO col_date, typ_date
    FROM information_schema.columns
   WHERE table_name='pool_day_data'
     AND column_name IN ('date','day','day_start','day_start_unix','started_at')
   ORDER BY ordinal_position LIMIT 1;

  IF col_f0 IS NULL OR col_f1 IS NULL OR col_date IS NULL THEN
    RAISE EXCEPTION 'Required columns not found in pool_day_data (fee0: %, fee1: %, date: %)', col_f0, col_f1, col_date;
  END IF;

  IF typ_date IN ('integer','bigint','numeric') THEN
    date_expr := 'to_timestamp(d.'||col_date||')::date';
  ELSE
    date_expr := 'd.'||quote_ident(col_date)||'::date';
  END IF;

  dyn_sql :=
    'drop view if exists v_pool_day_fees_usd_partial; '||
    'create view v_pool_day_fees_usd_partial as '||
    'select d.pool_id, '||date_expr||' as date, '||
    'case when rs0.is_stable then d.'||quote_ident(col_f0)||' '||
         'when rs1.is_stable then d.'||quote_ident(col_f1)||' '||
         'else null end as fees_usd '||
    'from pool_day_data d '||
    'join pools p on p.id=d.pool_id '||
    'join tokens t0 on t0.id=p.token0_id '||
    'join tokens t1 on t1.id=p.token1_id '||
    'left join ref_tokens rs0 on upper(t0.symbol)=rs0.symbol '||
    'left join ref_tokens rs1 on upper(t1.symbol)=rs1.symbol '||
    'where p.chain_id=1';
  EXECUTE dyn_sql;
END
$do$;
