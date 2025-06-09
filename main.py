import psycopg2
from dotenv import load_dotenv
import os


# Fetch variables
USER = "postgres.apphxbmngxlclxromyvt"
PASSWORD = "insightbot2025"
HOST = "aws-0-eu-north-1.pooler.supabase.com"
PORT = "5432"
DBNAME = "postgres"

# Connect to the database
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Connection successful!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    
    # Example query
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("Current Time:", result)

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed.")

except Exception as e:
    print(f"Failed to connect: {e}")
