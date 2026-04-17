create table if not exists pnl_snapshots (
    account_id varchar(64) not null,
    symbol varchar(32) not null,
    window_name varchar(16) not null,
    position_qty numeric(20, 8) not null,
    avg_cost numeric(20, 8) not null,
    last_price numeric(20, 8) not null,
    realized_pnl numeric(20, 8) not null,
    unrealized_pnl numeric(20, 8) not null,
    total_pnl numeric(20, 8) not null,
    updated_at timestamptz not null,
    primary key (account_id, symbol, window_name)
);
