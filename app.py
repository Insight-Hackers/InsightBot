import os
import json
from datetime import datetime
from flask import Flask, request
import psycopg2

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
    except psycopg2.OperationalError as e:
        print(f"❌ Database connection error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error in get_db_connection: {e}")
        raise

@app.route("/slack/events", methods=["POST"])
def slack_events():
    data = request.json
    print("📥 Full payload:")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    
    if "challenge" in data:
        return data["challenge"], 200

    event = data.get("event", {})
    if not event:
        print("❌ No 'event' key in payload")
        return "", 200

    event_type = event.get("type")
    print(f"🔍 Event type: {event_type}, subtype: {event.get('subtype', 'None')}")

    if event_type == "message" and "subtype" not in event:
        try:
            save_to_db(event, data)
            print("✅ הודעה נשמרה במסד בהצלחה")
        except Exception as e:
            print(f"❌ שגיאה בשמירת הודעה: {e}")
    elif event_type == "message" and event.get("subtype") == "message_deleted":
        try:
            delete_from_db(event)
            print("🗑 הודעה נמחקה מהמסד בהצלחה")
        except Exception as e:
            print(f"❌ שגיאה במעקב הודעה שנמחקה: {e}")
    elif event_type in ["reaction_added", "reaction_removed"]:
        try:
            save_to_db(event, data)
            print(f"✅ תגובה מסוג {event_type} נשמרה בהצלחה")
        except Exception as e:
            print(f"❌ שגיאה בשמירת תגובה ({event_type}): {e}")
    else:
        print(f"⚠️ Unhandled event type: {event_type}")

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
        elif block.get("type") == "rich_text":
            for element in block.get("elements", []):
                if element.get("type") == "rich_text_section":
                    for sub_element in element.get("elements", []):
                        if sub_element.get("type") == "text":
                            texts.append(sub_element.get("text", "").strip())
    return "\n".join(texts) if texts else None

def track_deleted_message(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        deleted_ts = event["deleted_ts"]
        current_ts = event.get("ts", str(datetime.now().timestamp()))
        
        print(f"🗑 Tracking deleted message: {deleted_ts}")
        
        cur.execute("""
            SELECT channel_id, user_id, text, thread_ts, parent_event_id, is_list, list_items, num_list_items
            FROM slack_messages_raw 
            WHERE event_id = %s
        """, (deleted_ts,))
        
        original_message = cur.fetchone()
        if not original_message:
            print(f"⚠️ No original message found for deleted_ts={deleted_ts}")
        
        delete_event_id = f"{deleted_ts}deleted{current_ts}"
        
        cur.execute("""
            INSERT INTO slack_messages_raw (
                event_id, channel_id, user_id, text, ts, thread_ts, raw, event_type, 
                parent_event_id, is_list, list_items, num_list_items
            ) VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
        """, (
            delete_event_id,
            original_message[0] if original_message else event.get("channel", "unknown"),
            event.get("user", "unknown"),
            f"🗑 הודעה נמחקה: {original_message[2] if original_message else 'תוכן לא ידוע'}",
            float(current_ts),
            original_message[3] if original_message else None,
            json.dumps(full_payload),
            "message_deleted",
            deleted_ts,
            original_message[5] if original_message else False,
            original_message[6] if original_message else None,
            original_message[7] if original_message else 0
        ))
        
        conn.commit()
        print(f"✅ Added delete tracking: {delete_event_id}")
        
    except Exception as e:
        print(f"❌ Error tracking deletion: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def save_to_db(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()

    event_type = event.get("type")
    is_reaction = event_type in ["reaction_added", "reaction_removed"]

    if event_type == "message" and event.get("thread_ts") and event.get("thread_ts") != event.get("ts"):
        event_type = "concatenation"

        ts_value = event.get("ts") or event.get("event_ts")
        if not ts_value:
            print("❌ No timestamp in event")
            return
            
        ts = float(ts_value)
        event_id = ts_value
        parent_event_id = None
        text = event.get("text") or extract_text_from_blocks(event.get("blocks")) or "No text available"

        print(f"💾 Saving message: {event_id}")
        print(f"📝 Text: {text}")

    if is_reaction:
        text = f":{event.get('reaction')}: by {event.get('user')}"
        parent_event_id = event["item"]["ts"]

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

        cur.execute("""
            INSERT INTO slack_messages_raw (
                event_id, channel_id, user_id, text, ts, thread_ts,
                raw, event_type, parent_event_id, is_list, list_items, num_list_items
            )
            VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
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

        if cur.rowcount == 0:
            print(f"⚠️ Skipped insert due to duplicate event_id: {event_id}")
        else:
            print(f"✅ Inserted event_id: {cur.fetchone()[0]}")

        conn.commit()
        print(f"✅ Transaction committed for event_id: {event_id}")
        
    except Exception as e:
        print(f"❌ Error saving to DB: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)