import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "jaffle_db",
    "user": "jaffle",
    "password": "jaffle"
}

SCHEMA = "main"

def get_table_names(conn, schema):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
            """,
            (schema,)
        )
        return [row[0] for row in cur.fetchall()]

def truncate_tables(conn, schema, tables):
    with conn.cursor() as cur:
        for table in tables:
            print(f"Truncating {schema}.{table} ...", flush=True)
            cur.execute(f'TRUNCATE TABLE "{schema}"."{table}" RESTART IDENTITY CASCADE;')
    conn.commit()
    print("All tables truncated.")

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        tables = get_table_names(conn, SCHEMA)
        if not tables:
            print(f"No tables found in schema '{SCHEMA}'.")
            return
        print(f"Found tables: {tables}")
        truncate_tables(conn, SCHEMA, tables)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 