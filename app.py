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
            password="insightbot2025",  # ✏ עדכני אם צריך
            host="aws-0-eu-north-1.pooler.supabase.com",
            port="5432"
        )
        print("🟢 התחברות למסד הצליחה")
        return conn
    except Exception as e:
        print("❌ שגיאה בהתחברות למסד:", e)
        raise


@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print("📥 כל ה-event מ-Slack:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    # בדיקה ספציפית לblocks
    if 'event' in data and 'blocks' in data['event']:
        print("✅ נמצא blocks!")
        print(f"מספר blocks: {len(data['event']['blocks'])}")
    else:
        print("❌ אין blocks ב-event")

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
    # elif event_type == "message" and event.get("subtype") == "message_deleted":
    #     try:
    #         delete_from_db(event)
    #         print("🗑 הודעה נמחקה מהמסד בהצלחה")
    #     except Exception as e:
    #         print("❌ שגיאה במחיקת הודעה מהמסד:", e)
    elif event_type in ["reaction_added", "reaction_removed"]:
        try:
            save_to_db(event, data)
            print(f"✅ תגובה מסוג {event_type} נשמרה בהצלחה")
        except Exception as e:
            print(f"❌ שגיאה בשמירת תגובה ({event_type}):", e)

    return "", 200

def extract_text_from_blocks(blocks):
    texts = []
    if not blocks:
        return None

    for block in blocks:
        if block.get("type") == "section":
            text_obj = block.get("text")
            if text_obj and text_obj.get("type") in ["plain_text", "mrkdwn"]:
                texts.append(text_obj.get("text", "").strip())

    return "\n".join(texts) if texts else None


def delete_from_db(event):
    conn = get_db_connection()
    cur = conn.cursor()

    deleted_ts = event["deleted_ts"]
    cur.execute("DELETE FROM slack_messages_raw WHERE event_id = %s", (deleted_ts,))

    conn.commit()
    cur.close()
    conn.close()

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
    text = event.get("text") or extract_text_from_blocks(event.get("blocks"))


    if is_reaction:
        text = f":{event.get('reaction')}: by {event.get('user')}"
        parent_event_id = event["item"]["ts"]

    # 🧠 ניתוח האם ההודעה היא רשימה
    is_list = False
    list_items = []
    num_list_items = 0

    if text:
        lines = text.splitlines()
        for line in lines:
            line = line.strip()
            if line.startswith(("* ", "- ", "• ")):
                is_list = True
                list_items.append(line[2:].strip())

        num_list_items = len(list_items) if is_list else 0

    # 📝 הכנסת הנתונים לטבלה
    cur.execute("""
        INSERT INTO slack_messages_raw (
            event_id, channel_id, user_id, text, ts, thread_ts,
            raw, event_type, parent_event_id,
            is_list, list_items, num_list_items
        )
        VALUES (%s, %s, %s, %s, to_timestamp(%s), %s,
                %s, %s, %s, %s, %s, %s)
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
        parent_event_id,
        is_list,
        json.dumps(list_items) if list_items else None,
        num_list_items
    ))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":  # ✅ נכוןשפפץ
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)