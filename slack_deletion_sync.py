from agent_monitor import load_slack_messages


def load_filtered_slack_messages():
    """
    טוען הודעות Slack לאחר סינון הודעות שנמחקו (event_type == 'message_deleted').
    אם העמודה לא קיימת – מחזיר את כל ההודעות.
    """
    df = load_slack_messages()

    if 'event_type' in df.columns:
        df = df[df['event_type'] != 'message_deleted']

    return df
