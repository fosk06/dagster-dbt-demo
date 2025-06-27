import duckdb
import os

# Toujours stocker les fichiers dans 'jaffle-data' à la racine du projet
DATA_DIR = "jaffle-data"

# Chemin du CSV source
csv_path = os.path.join(DATA_DIR, "raw_tweets.csv")

# Connexion DuckDB en mémoire
con = duckdb.connect()

# 1. Parquet "invalid" : tout en STRING
con.execute(f'''
    CREATE OR REPLACE TABLE raw_tweets_invalid AS
    SELECT * FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE)
''')
con.execute(f"COPY raw_tweets_invalid TO '{os.path.join(DATA_DIR, 'raw_tweets_invalid.parquet')}' (FORMAT PARQUET)")

# 2. Parquet "valid" : tweeted_at en TIMESTAMP, le reste en STRING
con.execute(f'''
    CREATE OR REPLACE TABLE raw_tweets_valid AS
    SELECT 
        id::VARCHAR AS id,
        user_id::VARCHAR AS user_id,
        tweeted_at::TIMESTAMP AS tweeted_at,
        content::VARCHAR AS content
    FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE)
''')
con.execute(f"COPY raw_tweets_valid TO '{os.path.join(DATA_DIR, 'raw_tweets.parquet')}' (FORMAT PARQUET)")

con.close()

print("✅ Parquet files written in jaffle-data/: raw_tweets_invalid.parquet (all string), raw_tweets_valid.parquet (tweeted_at as TIMESTAMP)")