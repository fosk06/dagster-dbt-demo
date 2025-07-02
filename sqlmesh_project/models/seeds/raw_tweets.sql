MODEL (
  name sqlmesh_jaffle_platform.raw_tweets,
  kind SEED (
    path '../../../jaffle-data/raw_tweets.csv'
  ),
  columns (
    id TEXT,
    user_id TEXT,
    tweeted_at TEXT,
    content TEXT
  ),
  grain (id)
); 