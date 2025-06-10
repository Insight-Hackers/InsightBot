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
            password="insightbot2025",
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

    elif event_type in ["reaction_added", "reaction_removed"]:
        try:
            save_to_db(event, data)
            print(f"✅ תגובה מסוג {event_type} נשמרה בהצלחה")
        except Exception as e:
            print(f"❌ שגיאה בשמירת תגובה ({event_type}):", e)

    return "", 200


def extract_text_from_blocks(blocks):
    """פונקציה משופרת לחילוץ טקסט מ-blocks כולל רשימות"""
    if not blocks:
        return None
    
    all_text = []
    
    for block in blocks:
        block_type = block.get("type")
        
        if block_type == "section":
            # טקסט רגיל
            text_obj = block.get("text")
            if text_obj and text_obj.get("type") in ["plain_text", "mrkdwn"]:
                all_text.append(text_obj.get("text", "").strip())
        
        elif block_type == "rich_text":
            # טיפול ב-rich_text blocks (כאן נמצאות הרשימות!)
            rich_text = extract_rich_text_blocks(block)
            if rich_text:
                all_text.append(rich_text)
    
    return "\n".join(all_text) if all_text else None


def extract_rich_text_blocks(rich_block):
    """חילוץ טקסט מ-rich_text blocks"""
    text_parts = []
    
    for element in rich_block.get('elements', []):
        element_type = element.get('type')
        
        if element_type == 'rich_text_section':
            # טקסט רגיל
            section_text = extract_rich_text_section(element)
            if section_text:
                text_parts.append(section_text)
        
        elif element_type == 'rich_text_list':
            # רשימות - זה החשוב!
            list_text = extract_rich_text_list(element)
            if list_text:
                text_parts.append(list_text)
    
    return '\n'.join(text_parts)


def extract_rich_text_section(section):
    """חילוץ טקסט מ-rich_text_section"""
    text_parts = []
    for element in section.get('elements', []):
        if element.get('type') == 'text':
            text_parts.append(element.get('text', ''))
        elif element.get('type') == 'link':
            url = element.get('url', '')
            text = element.get('text', url)
            text_parts.append(f"{text} ({url})")
    
    return ''.join(text_parts)


def extract_rich_text_list(list_element):
    """חילוץ רשימות מ-rich_text_list - הפונקציה החשובה!"""
    list_items = []
    style = list_element.get('style', 'bullet')
    
    for i, item in enumerate(list_element.get('elements', []), 1):
        if item.get('type') == 'rich_text_section':
            item_text = extract_rich_text_section(item)
            if item_text:
                if style == 'ordered':
                    list_items.append(f"{i}. {item_text}")
                else:
                    list_items.append(f"• {item_text}")
    
    return '\n'.join(list_items)


def detect_list_from_text(text):
    """זיהוי רשימות מטקסט - משופר"""
    if not text:
        return False, [], 0
    
    list_items = []
    lines = text.splitlines()
    
    for line in lines:
        line = line.strip()
        # זיהוי סוגי רשימות שונים
        if line.startswith(("* ", "- ", "• ", "◦ ", "▪ ")):
            list_items.append(line[2:].strip())
        elif line.startswith(("1. ", "2. ", "3. ", "4. ", "5. ", "6. ", "7. ", "8. ", "9. ")):
            # רשימה ממוספרת
            list_items.append(line[3:].strip())
        elif ". " in line[:5] and line[:line.index(". ")].isdigit():
            # רשימה ממוספרת גמישה יותר
            list_items.append(line[line.index(". ") + 2:].strip())
    
    is_list = len(list_items) > 0
    return is_list, list_items, len(list_items)


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
    
    # שילוב טקסט רגיל + טקסט מ-blocks
    regular_text = event.get("text", "")
    blocks_text = extract_text_from_blocks(event.get("blocks"))
    
    # שילוב הטקסטים
    if regular_text and blocks_text:
        text = f"{regular_text}\n{blocks_text}"
    elif blocks_text:
        text = blocks_text
    else:
        text = regular_text

    if is_reaction:
        text = f":{event.get('reaction')}: by {event.get('user')}"
        parent_event_id = event["item"]["ts"]

    # 🧠 ניתוח רשימות משופר
    is_list, list_items, num_list_items = detect_list_from_text(text)

    print(f"📝 טקסט מלא: {text}")
    print(f"📋 זוהתה רשימה: {is_list}, פריטים: {num_list_items}")

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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)