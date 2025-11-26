import duckdb

# Connect to a DuckDB database file (or create if not exists)
conn = duckdb.connect("movie_graph.duckdb")

# Create tables directly from CSVs
conn.execute("CREATE TABLE person AS SELECT * FROM read_csv_auto('persons.csv')")
conn.execute("CREATE TABLE movie AS SELECT * FROM read_csv_auto('movies.csv')")
conn.execute("CREATE TABLE acted_in AS SELECT * FROM read_csv_auto('acted_in.csv')")
conn.execute("CREATE TABLE directed AS SELECT * FROM read_csv_auto('directed.csv')")
conn.execute("CREATE TABLE follows AS SELECT * FROM read_csv_auto('follows.csv')")

# Print row counts
print("Table row counts:")
for t in ['person', 'movie', 'acted_in', 'directed', 'follows']:
    count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {count}")

conn.close()