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
            password="עדילוסטיג2025",  # ✏️ עדכני אם צריך
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
    event_type = event.get("type")

    if event_type == "message" and "subtype" not in event:
        try:
            save_to_db(event, data)
            print("✅ הודעה נשמרה במסד בהצלחה")
        except Exception as e:
            print("❌ שגיאה בשמירת הודעה:", e)

    elif event_type in ["reaction_added", "reaction_removed"]:
        try:
            save_to_db(event, data)
            print(f"✅ תגובה מסוג {event_type} נשמרה בהצלחה")
        except Exception as e:
            print(f"❌ שגיאה בשמירת תגובה ({event_type}):", e)

    return "", 200


def save_to_db(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()

    event_type = event.get("type")
    is_reaction = event_type in ["reaction_added", "reaction_removed"]
    if event_type == "message" and event.get("thread_ts") and event.get("thread_ts") != event.get("ts"):
        event_type = "concatenation"

    ts = float(event.get("ts") or event.get("event_ts"))
    event_id = event.get("ts") or event.get("event_ts")
    parent_event_id = None
    text = event.get("text")

    if is_reaction:
        text = f":{event.get('reaction')}: by {event.get('user')}"
        parent_event_id = event["item"]["ts"]

    cur.execute("""
        INSERT INTO slack_messages_raw (
            event_id, channel_id, user_id, text, ts, thread_ts, raw, event_type, parent_event_id
        )
        VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, (
        event_id,
        event["item"]["channel"] if is_reaction else event.get("channel"),
        event.get("user"),
        text,
        ts,
        event.get("thread_ts") if not is_reaction else None,
        json.dumps(full_payload),
        event_type,
        parent_event_id
    ))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
