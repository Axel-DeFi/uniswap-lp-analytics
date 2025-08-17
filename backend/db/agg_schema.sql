create table if not exists pool_day_data (
  id text primary key,
  pool_id text not null references pools(id) on delete cascade,
  date int not null,
  volume_token0 numeric,
  volume_token1 numeric,
  approx_fee_token0 numeric,
  approx_fee_token1 numeric,
  swap_count int not null default 0
);
create unique index if not exists uq_pool_day on pool_day_data(pool_id, date);
create index if not exists idx_pool_day_date on pool_day_data(date desc);

create table if not exists pool_hour_data (
  id text primary key,
  pool_id text not null references pools(id) on delete cascade,
  hour_start_unix int not null,
  volume_token0 numeric,
  volume_token1 numeric,
  approx_fee_token0 numeric,
  approx_fee_token1 numeric,
  swap_count int not null default 0
);
create unique index if not exists uq_pool_hour on pool_hour_data(pool_id, hour_start_unix);
create index if not exists idx_pool_hour_ts on pool_hour_data(hour_start_unix desc);
