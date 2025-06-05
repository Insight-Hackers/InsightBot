import os
from flask import Flask, request
import psycopg2
import json

app = Flask(__name__)
print("✅ הקובץ app.py התחיל לרוץ")


def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.apphxbmngxlclxromyvt",
            password="עדילוסטיג2025",  # ✏️ שימי את הסיסמה האמיתית שלך פה
            host="aws-0-eu-north-1.pooler.supabase.com",
            port="6543"
        )
        print("🟢 התחברות למסד הצליחה")
        return conn
    except Exception as e:
        print("❌ שגיאה בהתחברות למסד:", e)
        raise


@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print("📥 התקבלה בקשה מ-Slack:", json.dumps(data, indent=2))

    if "challenge" in data:
        return data["challenge"], 200

    event = data.get("event", {})
    if event.get("type") == "message" and "subtype" not in event:
        try:
            save_to_db(event, data)
            print("✅ הודעה נשמרה במסד בהצלחה")
        except Exception as e:
            print("❌ שגיאה בשמירת הודעה:", e)
    return "", 200


def save_to_db(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO slack_messages_raw (event_id, channel_id, user_id, text, ts, thread_ts, raw)
        VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, (
        event.get("ts"),  # משמש כ־event_id
        event.get("channel"),
        event.get("user"),
        event.get("text"),
        float(event.get("ts")),
        event.get("thread_ts"),
        json.dumps(full_payload)
    ))
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
