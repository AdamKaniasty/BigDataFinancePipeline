#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

reader = csv.DictReader(sys.stdin)

print("=" * 100)
print("CRYPTO PRICE DATA FROM HDFS")
print("=" * 100)
print()

row_count = 0
for row in reader:
    row_count += 1
    print(f"Record {row_count}:")
    print(f"  Symbol: {row.get('symbol', 'N/A')}")
    print(f"  Price: {row.get('price', 'N/A')}")
    print(f"  Price Change %: {row.get('price_change_percent', 'N/A')}")
    print(f"  High: {row.get('high_price', 'N/A')}")
    print(f"  Low: {row.get('low_price', 'N/A')}")
    print(f"  Open: {row.get('open_price', 'N/A')}")
    print(f"  Volume: {row.get('volume', 'N/A')}")
    print(f"  Quote Volume: {row.get('quote_volume', 'N/A')}")
    
    # Format event time
    event_time = row.get('event_time', '')
    if event_time:
        try:
            ts = int(event_time) / 1000
            dt = datetime.fromtimestamp(ts)
            print(f"  Event Time: {dt.strftime('%Y-%m-%d %H:%M:%S')} ({event_time})")
        except:
            print(f"  Event Time: {event_time}")
    else:
        print(f"  Event Time: N/A")
    
    print()

print("=" * 100)
print(f"Total records: {row_count}")
print("=" * 100)

