import os
from flask import Flask, request
import psycopg2
import json
from datetime import datetime

app = Flask(__name__)  # תיקון: __name_ במקום name
print("✅ הקובץ app.py התחיל לרוץ")


def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.apphxbmngxlclxromyvt",
            password="insightbot2025",
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
    
    print(f"🔍 סוג האירוע: {event_type}")
    print(f"🔍 subtype: {event.get('subtype', 'אין')}")

    if event_type == "message" and "subtype" not in event:
        try:
            save_to_db(event, data)
            print("✅ הודעה נשמרה במסד בהצלחה")
        except Exception as e:
            print("❌ שגיאה בשמירת הודעה:", e)
            print(f"📋 פרטי השגיאה: {str(e)}")
    elif event_type == "message" and event.get("subtype") == "message_deleted":
        try:
            track_deleted_message(event, data)
            print("🗑 הודעה שנמחקה נוספה למעקב בהצלחה")
        except Exception as e:
            print("❌ שגיאה במעקב הודעה שנמחקה:", e)
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


def track_deleted_message(event, full_payload):
    """
    מוסיף רשומה חדשה עבור הודעה שנמחקה במקום למחוק את הרשומה הקיימת
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        deleted_ts = event["deleted_ts"]
        current_ts = event.get("ts", str(datetime.now().timestamp()))
        
        print(f"🗑 מעקב הודעה שנמחקה: {deleted_ts}")
        
        # שליפת מידע על ההודעה המקורית
        cur.execute("""
            SELECT channel_id, user_id, text, thread_ts, parent_event_id, is_list, list_items, num_list_items
            FROM slack_messages_raw 
            WHERE event_id = %s
        """, (deleted_ts,))
        
        original_message = cur.fetchone()
        
        # יצירת event_id חדש עבור אירוע המחיקה
        delete_event_id = f"{deleted_ts}deleted{current_ts}"
        
        # הוספת רשומה עבור אירוע המחיקה
        cur.execute("""
            INSERT INTO slack_messages_raw (
                event_id, 
                channel_id, 
                user_id, 
                text, 
                ts, 
                thread_ts, 
                raw, 
                event_type, 
                parent_event_id,
                is_list,
                list_items,
                num_list_items
            ) VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
        """, (
            delete_event_id,
            original_message[0] if original_message else event.get("channel"),
            event.get("user"),  # המשתמש שביצע את המחיקה
            f"🗑 הודעה נמחקה: {original_message[2] if original_message else 'תוכן לא ידוע'}",
            float(current_ts),
            original_message[3] if original_message else None,
            json.dumps(full_payload),
            "message_deleted",
            deleted_ts,  # קישור לאירוע המקורי
            original_message[5] if original_message else False,
            original_message[6] if original_message else None,
            original_message[7] if original_message else 0
        ))
        
        conn.commit()
        print(f"✅ נוסף מעקב למחיקה: {delete_event_id}")
        
    except Exception as e:
        print(f"❌ שגיאה בהוספת מעקב מחיקה: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def save_to_db(event, full_payload):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        event_type = event.get("type")
        is_reaction = event_type in ["reaction_added", "reaction_removed"]

        if event_type == "message" and event.get("thread_ts") and event.get("thread_ts") != event.get("ts"):
            event_type = "concatenation"

        # תיקון: הוספת בדיקה לts
        ts_value = event.get("ts") or event.get("event_ts")
        if not ts_value:
            print("❌ לא נמצא ts בעירוע")
            return
            
        ts = float(ts_value)
        event_id = ts_value
        parent_event_id = None
        text = event.get("text") or extract_text_from_blocks(event.get("blocks"))

        print(f"💾 שומר הודעה: {event_id}")
        print(f"📝 טקסט: {text}")

        if is_reaction:
            text = f":{event.get('reaction')}: by {event.get('user')}"
            parent_event_id = event["item"]["ts"]

        # ניתוח האם ההודעה היא רשימה
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

        # הכנסת הנתונים לטבלה
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
        print(f"✅ נתונים נשמרו בהצלחה: {event_id}")
        
    except Exception as e:
        print(f"❌ שגיאה בשמירה: {e}")
        print(f"📋 פרטי השגיאה המלאים: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "_main":  # תיקון: __name_ ו _main_ במקום name ו main
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)  # הוספת debug=True לבדיקות