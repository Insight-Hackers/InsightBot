import requests
import psycopg2
import os

# 🔐 החלף לפרטים שלך
MONDAY_API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUyNjUzNzkxMSwiYWFpIjoxMSwidWlkIjo3NjIwODA1NSwiaWFkIjoiMjAyNS0wNi0xNVQyMzo1NjoxOC4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6Mjk1OTE3MTcsInJnbiI6ImV1YzEifQ.lrtEmRx5OoFJEpW7l4EFm_VMaw05e3eJvUm6bG-RCy8"
MONDAY_BOARD_ID = "2049692456"
SUPABASE_CONN_STRING = "postgresql://user:password@host:port/dbname"


def fetch_monday_items():
    query = {
        "query": f"""
        {{
            boards(ids: {MONDAY_BOARD_ID}) {{
                items {{
                    id
                    name
                    column_values {{
                        id
                        text
                    }}
                }}
            }}
        }}
        """
    }

    headers = {
        "Authorization": MONDAY_API_TOKEN
    }

    response = requests.post("https://api.monday.com/v2",
                             json=query, headers=headers)
    response.raise_for_status()
    return response.json()["data"]["boards"][0]["items"]


def update_supabase(items):
    conn = psycopg2.connect(SUPABASE_CONN_STRING)
    cur = conn.cursor()

    for item in items:
        item_id = int(item["id"])
        item_name = item["name"]
        for col in item["column_values"]:
            col_id = col["id"]
            val = col.get("text", "")
            cur.execute("""
                INSERT INTO monday_items (item_id, item_name, column_id, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (item_id, column_id) DO UPDATE
                SET value = EXCLUDED.value,
                    item_name = EXCLUDED.item_name,
                    updated_at = now();
            """, (item_id, item_name, col_id, val))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    items = fetch_monday_items()
    update_supabase(items)
