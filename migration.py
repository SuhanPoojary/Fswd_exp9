from pymongo import MongoClient
import psycopg2

# Connect to MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")  # or your Mongo URI
mongo_db = mongo_client["library"]
mongo_collection = mongo_db["books"]

# Connect to PostgreSQL
pg_conn = psycopg2.connect(
    dbname="Library",
    user="postgres",
    password="Suhan",  # replace with your PostgreSQL password
    host="localhost",
    port="5432"
)
pg_cursor = pg_conn.cursor()

# Fetch documents from MongoDB and insert into PostgreSQL
for doc in mongo_collection.find():
    title = doc.get("title")
    author = doc.get("author")
    year = doc.get("year")

    pg_cursor.execute("""
        INSERT INTO books (title, author, year_published)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (title, author, year))

# Commit and close
pg_conn.commit()
pg_cursor.close()
pg_conn.close()
mongo_client.close()

print("✅ Data migrated successfully from MongoDB to PostgreSQL!")
