create table if not exists tokens (
  id text primary key,
  address text not null,
  symbol text,
  name text,
  decimals int not null,
  chain_id int not null,
  created_at_ts bigint not null
);

create unique index if not exists uq_tokens_addr_chain on tokens(lower(address), chain_id);

create table if not exists pools (
  id text primary key,
  version smallint not null,
  chain_id int not null,
  token0_id text not null references tokens(id) on delete restrict,
  token1_id text not null references tokens(id) on delete restrict,
  fee_tier_bps int not null,
  tick_spacing int not null,
  created_at_ts bigint not null
);

create index if not exists idx_pools_chain_ver on pools(chain_id, version);
create index if not exists idx_pools_tokens on pools(token0_id, token1_id);
create index if not exists idx_pools_created on pools(created_at_ts desc);
