from typing import List, Tuple
from database import get_db_connection
from db_operations import (
    get_sync_token, 
    update_sync_token, 
    delete_sync_token,
    save_event_to_db,
    get_event_by_id,
    update_event_status,
    update_event_change_history
)
from datetime import datetime


def parse_event_time(time_str: str) -> datetime:
    if 'T' in time_str:
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    else:
        return datetime.fromisoformat(time_str) 

def detect_changes(old_event: Tuple, new_event: dict) -> List[str]:
    """
    Detect what changed in an event
    """
    changes = []
    
    # Check if event was cancelled
    if new_event.get('status') == 'cancelled':
        changes.append('CANCELLED')
        return changes
    
    # Get old and new start times
    old_start = old_event[2]  
    new_start_raw = new_event['start'].get('dateTime', new_event['start'].get('date'))
    old_start_str = str(old_start)[:19] if old_start else "" 
    if 'T' in str(new_start_raw):
        new_start_str = str(new_start_raw).split('T')[0] + ' ' + str(new_start_raw).split('T')[1][:8]
    else:
        new_start_str = str(new_start_raw)
    
    if old_start_str != new_start_str:
        changes.append('TIME_CHANGED')
    
    # Check attendee changes
    old_att_count = old_event[6] if old_event[6] else 0
    new_att_count = len(new_event.get('attendees', []))
    
    if old_att_count != new_att_count:
        if new_att_count > old_att_count:
            changes.append('ATTENDEE_ADDED')
        else:
            changes.append('ATTENDEE_REMOVED')
    
    # Check title change
    old_summary = old_event[0] if old_event[0] else ""
    new_summary = new_event.get('summary', "")
    
    if old_summary != new_summary:
        changes.append('TITLE_CHANGED')
    
    return changes

def check_for_updates(service, user_id: int) -> int:
    """
    Check for calendar updates using sync tokens
    """
    sync_token = get_sync_token(user_id)
    
    try:
        if sync_token:
            #  syncing token for updates         
            events_result = service.events().list(
                calendarId='primary',
                syncToken=sync_token
            ).execute()

        else:
            events_result = service.events().list(
                calendarId='primary',
                maxResults=2500
            ).execute()
        
        events = events_result.get('items', [])
        new_sync_token = events_result.get('nextSyncToken')
        
        if not events and not new_sync_token:
            return 0
    
        conn = get_db_connection()
        cursor = conn.cursor()      
        changes_count = 0
        
        for event in events:
            event_id = event.get('id')
            if event.get('status') == 'cancelled':
                old_event_data = get_event_by_id(cursor, event_id)
                if old_event_data:
                    update_event_status(cursor, event_id, ['CANCELLED'])
                    changes_count += 1
                    print(f"\nEvent cancelled: '{event.get('summary', 'No Title')}'")
                continue
            
            old_event_data = get_event_by_id(cursor, event_id)
            
            if old_event_data:
                # Existing event check for changes
                changes = detect_changes(old_event_data, event)
                
                if changes:
                    changes_count += 1
                    print(f"\nEvent '{event.get('summary', 'No Title')}'")
                    print(f"   Changes: {', '.join(changes)}")
                    
                    save_event_to_db(cursor, user_id, event)
                    update_event_change_history(cursor, event_id, changes)
            else:
                if sync_token: 
                    print(f"\nNew event: '{event.get('summary', 'No Title')}'")
                    changes_count += 1
                save_event_to_db(cursor, user_id, event)
        
        if new_sync_token:
            update_sync_token(user_id, new_sync_token)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return changes_count
        
    except Exception as e:
        error_msg = str(e)
        if 'Sync token is no longer valid' in error_msg or 'invalid' in error_msg.lower():
            print("Sync token expired, performing full sync...")
            delete_sync_token(user_id)
            return check_for_updates(service, user_id)
        else:
            print(f"Error: {e}")
            return 0
        
    finally:
        cursor.close()
        conn.close()
