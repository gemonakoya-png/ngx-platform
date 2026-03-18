-- ============================================================
--  NGX Investment Platform — Supabase Database Schema
--  Paste this entire file into Supabase > SQL Editor > Run
--  Order matters — run top to bottom.
-- ============================================================


-- ────────────────────────────────────────────────────────────
--  TABLE 1: stocks
--  Master list of every NGX company on the platform.
--  One row per company. Updated when you add/remove stocks.
-- ────────────────────────────────────────────────────────────

create table if not exists stocks (
  id              uuid primary key default gen_random_uuid(),

  -- Identity
  ticker          text not null unique,   -- e.g. "GTCO.LG"
  name            text not null,          -- e.g. "Guaranty Trust Holding Co"
  sector          text not null,          -- e.g. "Banking"

  -- Exchange info
  exchange        text default 'NGX',
  currency        text default 'NGN',

  -- Basic company info (optional, enrichable later)
  description     text,
  website         text,
  founded_year    int,
  employees       int,

  -- Status
  is_active       boolean default true,   -- false = suspended / delisted
  created_at      timestamptz default now(),
  updated_at      timestamptz default now()
);

-- Indexes for common filters
create index if not exists idx_stocks_sector  on stocks (sector);
create index if not exists idx_stocks_active  on stocks (is_active);

-- Seed the 20 NGX stocks from our pipeline
insert into stocks (ticker, name, sector) values
  ('GTCO.LG',      'Guaranty Trust Holding Co',   'Banking'),
  ('ZENITHBANK.LG','Zenith Bank',                  'Banking'),
  ('ACCESS.LG',    'Access Holdings',              'Banking'),
  ('UBA.LG',       'United Bank for Africa',       'Banking'),
  ('FBNH.LG',      'FBN Holdings',                 'Banking'),
  ('STANBIC.LG',   'Stanbic IBTC Holdings',        'Banking'),
  ('NESTLE.LG',    'Nestle Nigeria',               'Consumer Goods'),
  ('NB.LG',        'Nigerian Breweries',           'Consumer Goods'),
  ('UNILEVER.LG',  'Unilever Nigeria',             'Consumer Goods'),
  ('DANGSUGAR.LG', 'Dangote Sugar Refinery',       'Consumer Goods'),
  ('DANGCEM.LG',   'Dangote Cement',               'Industrial'),
  ('BUACEMENT.LG', 'BUA Cement',                   'Industrial'),
  ('WAPCO.LG',     'Lafarge Africa',               'Industrial'),
  ('SEPLAT.LG',    'Seplat Energy',                'Oil & Gas'),
  ('CONOIL.LG',    'Conoil',                       'Oil & Gas'),
  ('MTNN.LG',      'MTN Nigeria',                  'Telecom'),
  ('AIRTELAFRI.LG','Airtel Africa',                'Telecom'),
  ('FIDSON.LG',    'Fidson Healthcare',            'Healthcare'),
  ('MAYBAKER.LG',  'May & Baker Nigeria',          'Healthcare'),
  ('AIICO.LG',     'AIICO Insurance',              'Insurance')
on conflict (ticker) do nothing;


-- ────────────────────────────────────────────────────────────
--  TABLE 2: prices
--  Daily closing prices for every stock.
--  One row per stock per trading day.
-- ────────────────────────────────────────────────────────────

create table if not exists prices (
  id              uuid primary key default gen_random_uuid(),

  ticker          text not null references stocks(ticker) on delete cascade,
  price_date      date not null,

  -- OHLCV (Open High Low Close Volume)
  open_price      numeric(12, 4),
  high_price      numeric(12, 4),
  low_price       numeric(12, 4),
  close_price     numeric(12, 4) not null,
  volume          bigint,

  -- Computed returns (populated by pipeline)
  ret_1d          numeric(8, 6),   -- 1-day return  e.g. 0.0123 = +1.23%
  ret_1w          numeric(8, 6),   -- 1-week return
  ret_1m          numeric(8, 6),   -- 1-month return
  ret_3m          numeric(8, 6),   -- 3-month return
  ret_6m          numeric(8, 6),   -- 6-month return
  ret_1y          numeric(8, 6),   -- 1-year return

  -- Moving averages
  ma_50           numeric(12, 4),
  ma_200          numeric(12, 4),
  above_ma50      boolean,
  above_ma200     boolean,

  -- Volatility & momentum
  rsi_14          numeric(6, 2),   -- RSI oscillator 0–100
  volatility_ann  numeric(8, 6),   -- Annualised std of daily returns

  -- 52-week range
  high_52w        numeric(12, 4),
  low_52w         numeric(12, 4),
  pct_from_high   numeric(8, 6),
  pct_from_low    numeric(8, 6),

  -- Timestamps
  created_at      timestamptz default now(),

  -- Each ticker can only have one row per date
  unique (ticker, price_date)
);

-- Indexes for fast date-range and ticker queries
create index if not exists idx_prices_ticker     on prices (ticker);
create index if not exists idx_prices_date       on prices (price_date desc);
create index if not exists idx_prices_ticker_date on prices (ticker, price_date desc);


-- ────────────────────────────────────────────────────────────
--  TABLE 3: fundamentals
--  Financial ratios from company filings.
--  One row per stock per fetch date (weekly/quarterly refresh).
-- ────────────────────────────────────────────────────────────

create table if not exists fundamentals (
  id              uuid primary key default gen_random_uuid(),

  ticker          text not null references stocks(ticker) on delete cascade,
  fetch_date      date not null,

  -- Valuation ratios
  pe_ratio        numeric(10, 4),   -- Price / Earnings
  pb_ratio        numeric(10, 4),   -- Price / Book value
  ps_ratio        numeric(10, 4),   -- Price / Sales
  ev_ebitda       numeric(10, 4),   -- Enterprise Value / EBITDA

  -- Profitability
  roe             numeric(10, 6),   -- Return on Equity  e.g. 0.22 = 22%
  roa             numeric(10, 6),   -- Return on Assets
  profit_margin   numeric(10, 6),   -- Net profit margin
  gross_margin    numeric(10, 6),   -- Gross margin

  -- Financial health
  debt_to_equity  numeric(10, 4),   -- Total debt / equity
  current_ratio   numeric(10, 4),   -- Current assets / liabilities
  quick_ratio     numeric(10, 4),   -- (Current assets - inventory) / liabilities

  -- Growth
  revenue_growth  numeric(10, 6),   -- YoY revenue growth
  earnings_growth numeric(10, 6),   -- YoY earnings growth

  -- Dividends & size
  dividend_yield  numeric(10, 6),   -- Annual dividend / price
  market_cap      bigint,           -- In NGN (naira)
  shares_outstanding bigint,

  -- Timestamps
  fetched_at      timestamptz default now(),
  created_at      timestamptz default now(),

  unique (ticker, fetch_date)
);

create index if not exists idx_fundamentals_ticker on fundamentals (ticker);
create index if not exists idx_fundamentals_date   on fundamentals (fetch_date desc);


-- ────────────────────────────────────────────────────────────
--  TABLE 4: scores
--  Daily Smart Scores — the core output of the platform.
--  One row per stock per scoring date.
-- ────────────────────────────────────────────────────────────

create table if not exists scores (
  id              uuid primary key default gen_random_uuid(),

  ticker          text not null references stocks(ticker) on delete cascade,
  score_date      date not null,

  -- Sub-scores (each out of their max weight)
  fundamental_score  numeric(6, 2),   -- max 35
  momentum_score     numeric(6, 2),   -- max 25
  value_score        numeric(6, 2),   -- max 25
  growth_score       numeric(6, 2),   -- max 15

  -- Final composite score
  smart_score        numeric(6, 2) not null,   -- 0–100

  -- Human-readable signal
  signal             text not null,   -- 'Strong Buy' | 'Buy' | 'Hold' | 'Caution' | 'Avoid'
  risk_flag          text default 'Normal',    -- 'Normal' | 'High Risk'

  -- Snapshot of key metrics at scoring time (for display without joins)
  current_price      numeric(12, 4),
  ret_1m_pct         numeric(8, 2),   -- e.g. 5.3 means +5.3%
  ret_3m_pct         numeric(8, 2),
  dividend_yield_pct numeric(8, 2),
  pe_ratio           numeric(10, 4),
  roe_pct            numeric(8, 2),

  -- Timestamps
  created_at         timestamptz default now(),

  unique (ticker, score_date)
);

create index if not exists idx_scores_ticker     on scores (ticker);
create index if not exists idx_scores_date       on scores (score_date desc);
create index if not exists idx_scores_smart      on scores (smart_score desc);
create index if not exists idx_scores_signal     on scores (signal);


-- ────────────────────────────────────────────────────────────
--  VIEW: latest_scores
--  Convenience view — most recent score for every stock.
--  This is what your dashboard will query most often.
-- ────────────────────────────────────────────────────────────

create or replace view latest_scores as
select
  s.ticker,
  st.name,
  st.sector,
  s.score_date,
  s.smart_score,
  s.signal,
  s.risk_flag,
  s.fundamental_score,
  s.momentum_score,
  s.value_score,
  s.growth_score,
  s.current_price,
  s.ret_1m_pct,
  s.ret_3m_pct,
  s.dividend_yield_pct,
  s.pe_ratio,
  s.roe_pct
from scores s
join stocks st on st.ticker = s.ticker
where s.score_date = (
  select max(score_date) from scores s2 where s2.ticker = s.ticker
)
order by s.smart_score desc;


-- ────────────────────────────────────────────────────────────
--  ROW LEVEL SECURITY (RLS)
--  Allows the public to READ data (for the dashboard),
--  but only your pipeline (service role key) can WRITE.
-- ────────────────────────────────────────────────────────────

alter table stocks       enable row level security;
alter table prices       enable row level security;
alter table fundamentals enable row level security;
alter table scores       enable row level security;

-- Public read access (your dashboard users can see everything)
create policy "Public read stocks"        on stocks        for select using (true);
create policy "Public read prices"        on prices        for select using (true);
create policy "Public read fundamentals"  on fundamentals  for select using (true);
create policy "Public read scores"        on scores        for select using (true);

-- Only the service role (your pipeline) can insert / update / delete
-- (No insert policy = only service_role key can write. Anon/public users cannot.)


-- ────────────────────────────────────────────────────────────
--  DONE
--  You should now see 4 tables and 1 view in your Table Editor:
--    stocks · prices · fundamentals · scores · latest_scores
-- ────────────────────────────────────────────────────────────
