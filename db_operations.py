
# Database operations for users, events, and sync tokens

import json
from typing import Optional, List, Tuple
from database import get_db_connection
from models import User, Event, Attendee

#might need changes here 
def get_or_create_user(cursor, email: str, name: str) -> int:
    """Get existing user or create new one"""
    cursor.execute("""
        INSERT INTO users (email, name)
        VALUES (%s, %s)
        ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
        RETURNING id;
    """, (email, name))
    user_id = cursor.fetchone()
    
    if not user_id:
        cursor.execute("SELECT id FROM users WHERE email=%s", (email,))
        user_id = cursor.fetchone()
    return user_id[0]


def get_sync_token(user_id: int) -> Optional[str]: 
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sync_token FROM sync_tokens WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else None


def update_sync_token(user_id: int, new_sync_token: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO sync_tokens (user_id, sync_token, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            sync_token = EXCLUDED.sync_token,
            updated_at = NOW();
    """, (user_id, new_sync_token))
    conn.commit()
    cursor.close()
    conn.close()


def delete_sync_token(user_id: int, cursor):
    cursor.execute("DELETE FROM sync_tokens WHERE user_id = %s", (user_id,))

def save_event_to_db(cursor, user_id: int, event: dict):
    """Save or update an event in the database"""
    cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM events")
    new_id = cursor.fetchone()[0]

    start = event['start'].get('dateTime', event['start'].get('date'))
    end = event['end'].get('dateTime', event['end'].get('date'))
    summary = event.get('summary', 'No Title')
    description = event.get('description', '')
    status = event.get('status', 'confirmed')
    
    attendees = event.get('attendees', [])
    attendees_count = len(attendees)
    attendees_list = json.dumps([{
        'email': a.get('email'), 
        'responseStatus': a.get('responseStatus'),
        'displayName': a.get('displayName', '')
    } for a in attendees])

    cursor.execute("""
        INSERT INTO events (id, user_id, event_id, summary, description, start_time, end_time, 
                            status, attendees_count, attendees_list, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (event_id) DO UPDATE SET
            summary = EXCLUDED.summary,
            description = EXCLUDED.description,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            status = EXCLUDED.status,
            attendees_count = EXCLUDED.attendees_count,
            attendees_list = EXCLUDED.attendees_list,
            updated_at = NOW();
    """, (new_id, user_id, event['id'], summary, description, start, end, 
          status, attendees_count, attendees_list))

def get_event_by_id(cursor, event_id: str) -> Optional[Tuple]:
    cursor.execute("""
        SELECT summary, description, start_time, end_time, status, attendees_list, attendees_count
        FROM events WHERE event_id = %s
    """, (event_id,))
    return cursor.fetchone()


def update_event_status(cursor, event_id: str, changes: List[str]):
    cursor.execute("""
        UPDATE events 
        SET status = 'cancelled', 
            updated_at = NOW(),
            change_history = COALESCE(change_history || E'\n', '') || %s
        WHERE event_id = %s
    """, (', '.join(changes), event_id))


def update_event_change_history(cursor, event_id: str, changes: List[str]):
    cursor.execute("""
        UPDATE events 
        SET change_history = COALESCE(change_history || E'\n', '') || %s
        WHERE event_id = %s
    """, (', '.join(changes), event_id))
