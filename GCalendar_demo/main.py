"""
Google Calendar Monitor
"""
from __future__ import print_function
import time
from datetime import datetime
from config import settings
from database import setup_database, get_db_connection
from google_auth import (
    get_credentials, 
    get_calendar_service, 
    get_user_service,
    get_user_info
)
from db_operations import get_or_create_user
from calendar_sync import check_for_updates

#might need changes here 
def main():
    """Main application loop"""
    print("=" * 80)
    print(" Google Calendar Monitor - Simple Polling")
    print("=" * 80)
    
    setup_database()
    
    creds = get_credentials()
    calendar_service = get_calendar_service(creds)
    user_service = get_user_service(creds)
    user_info = get_user_info(user_service)
    
    print(f"\nMonitoring calendar for: {user_info.name} ({user_info.email})")
    print(f" Checking every {settings.CHECK_INTERVAL} seconds")
    print(f" Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n Press Ctrl+C to stop monitoring...\n")
    print("=" * 80)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    user_id = get_or_create_user(cursor, user_info.email, user_info.name)
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n Performing initial sync...")
    initial_changes = check_for_updates(calendar_service, user_id)
    print(f"Initial sync complete! Found {initial_changes} events\n")
    check_count = 1
    
    try:
        while True:
            time.sleep(settings.CHECK_INTERVAL)
            
            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp}] Check #{check_count}: ", end='')
            
            changes = check_for_updates(calendar_service, user_id)
            
            if changes > 0:
                print(f"Found {changes} change(s)")
            else:
                print("No changes")
            check_count += 1
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
        print(f"Total checks performed: {check_count - 1}")
        print("=" * 80)

if __name__ == '__main__':
    main()