import csv
import time

# Step 1: Read output.csv and build a dictionary mapping Image ID to Maintainer
output_dict = {}
with open('./output.csv', newline='', encoding='utf-8') as out_csv:
    reader = csv.DictReader(out_csv)
    for row in reader:
        image_id = row.get('Image ID')
        maintainer = row.get('Maintainer')
        if image_id is not None:
            output_dict[image_id] = maintainer

# Step 2: Open v2-report.csv and merged-report.csv
with open('./v2-report.csv', newline='', encoding='utf-8') as v2_csv, \
     open('merged-report.csv', 'w', newline='', encoding='utf-8') as merged_csv:

    v2_reader = csv.DictReader(v2_csv)
    fieldnames = v2_reader.fieldnames + ['Maintainer'] if v2_reader.fieldnames else ['Maintainer']
    writer = csv.DictWriter(merged_csv, fieldnames=fieldnames)
    writer.writeheader()

    # Initialize counters and timer
    row_count = 0
    match_count = 0
    start_time = time.time()
    
    print(f"Starting merge process at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    for row in v2_reader:
        row_count += 1
        image_id = row.get('Image ID')
        maintainer = output_dict.get(image_id, '')
        
        # Count matches (non-empty maintainer values)
        if maintainer:
            match_count += 1
            
        row['Maintainer'] = maintainer
        
        # Filter out any None keys that can cause writerow errors
        filtered_row = {k: v for k, v in row.items() if k is not None}
        writer.writerow(filtered_row)
        
        # Progress report every 10,000 rows
        if row_count % 10000 == 0:
            elapsed_time = time.time() - start_time
            print(f"Processed {row_count:,} rows in {elapsed_time:.2f} seconds - {match_count:,} maintainer matches written")

    # Final summary
    total_time = time.time() - start_time
    print(f"\nCompleted! Processed {row_count:,} total rows in {total_time:.2f} seconds")
