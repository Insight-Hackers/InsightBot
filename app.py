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
            user="postgres",
            password="insightbot2025",
            host="db.apphxbmngxlclxromyvt.supabase.co",
            port="5432"
        )
        print("🟢 התחברות למסד הצליחה")
        return conn
    except Exception as e:
        print("❌ שגיאה בהתחברות למסד:", e)
        raise


@app.route("/slack/events", methods=["POST"])
def slack_events():
    print("🔥 קיבלתי POST מ־Slack")
    print("🔥 request.data:", request.data)
    print("🔥 request.content_type:", request.content_type)

    try:
        data = request.get_json(force=True)
        print("📥 JSON שהתקבל:", json.dumps(data, indent=2))
    except Exception as e:
        print("❌ שגיאה בפרסור JSON:", e)
        return "Bad Request", 400

    if "challenge" in data:
        return data["challenge"], 200

    event = data.get("event", {})
    if event.get("type") == "message" and "subtype" not in event:
        try:
            save_to_db(event, data)
            print("✅ הודעה נשמרה במסד בהצלחה")
        except Exception as e:
            print("❌ שגיאה בשמירת הודעה:", e)
    else:
        print("ℹ️ האירוע שהתקבל אינו הודעת טקסט רגילה")

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


@app.route("/", methods=["GET", "HEAD"])
def root():
    return "👋 InsightBot Flask API פעיל", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
