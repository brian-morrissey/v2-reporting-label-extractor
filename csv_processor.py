import csv
import json
import time
from collections import defaultdict

def process_csv_file(csv_file_path, max_rows=None):
    """
    Process CSV file to extract unique Image ID and Container Labels/MAINTAINER pairs
    
    Args:
        csv_file_path (str): Path to the CSV file
        max_rows (int, optional): Maximum number of rows to process (None for all rows)
    
    Returns:
        dict: Dictionary with unique Image ID -> MAINTAINER mappings
    """
    unique_entries = {}
    missing_maintainer_count = 0
    total_rows = 0
    maintainer_found_count = 0
    
    print(f"Starting to process the CSV file: {csv_file_path}")
    if max_rows:
        print(f"Processing first {max_rows:,} rows...")
    else:
        print("Processing all rows...")
    
    start_time = time.time()
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            
            for row in csv_reader:
                total_rows += 1
                
                # Stop if max_rows limit is reached
                if max_rows and total_rows > max_rows:
                    break
                
                # Progress update every 10000 rows
                if total_rows % 10000 == 0:
                    elapsed = time.time() - start_time
                    print(f"Processed {total_rows:,} rows... ({elapsed:.1f}s elapsed)")
                
                # Extract Image ID and Container Labels
                image_id = row.get('Image ID', '') or ''
                container_labels_str = row.get('Container Labels', '') or ''
                
                # Strip whitespace if not None
                image_id = image_id.strip() if image_id else ''
                container_labels_str = container_labels_str.strip() if container_labels_str else ''
                
                # Skip rows with empty Image ID
                if not image_id:
                    continue
                
                # Parse Container Labels JSON
                maintainer = None
                if container_labels_str:
                    try:
                        container_labels = json.loads(container_labels_str)
                        # Look for MAINTAINER key
                        for key, value in container_labels.items():
                            if key == 'MAINTAINER':
                                maintainer = value
                                maintainer_found_count += 1
                                break
                    except json.JSONDecodeError:
                        # If JSON parsing fails, skip this entry
                        continue
                
                # Count entries without MAINTAINER
                if maintainer is None:
                    missing_maintainer_count += 1
                
                # Create unique key combination (image_id, maintainer)
                key = (image_id, maintainer)
                if key not in unique_entries:
                    unique_entries[key] = {
                        'image_id': image_id,
                        'maintainer': maintainer
                    }
    
    except FileNotFoundError:
        print(f"Error: File '{csv_file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error processing file: {e}")
        return None
    
    elapsed_total = time.time() - start_time
    print(f"\nProcessing completed in {elapsed_total:.1f} seconds")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Rows with MAINTAINER: {maintainer_found_count:,}")
    print(f"Rows without MAINTAINER: {missing_maintainer_count:,}")
    
    return unique_entries

def create_final_dictionary(unique_entries):
    """
    Create final dictionary with Image ID -> MAINTAINER mappings
    
    Args:
        unique_entries (dict): Dictionary from process_csv_file function
    
    Returns:
        dict: Final dictionary with Image ID -> MAINTAINER mappings
    """
    # Separate entries with and without MAINTAINER
    entries_with_maintainer = []
    entries_without_maintainer = []
    
    for key, value in unique_entries.items():
        image_id, maintainer = key
        if maintainer:
            entries_with_maintainer.append((image_id, maintainer))
        else:
            entries_without_maintainer.append((image_id, maintainer))
    
    # Create final dictionary with only entries that have MAINTAINER
    final_dict = {}
    for image_id, maintainer in entries_with_maintainer:
        final_dict[image_id] = maintainer
    
    print(f"\nFINAL RESULTS:")
    print("=" * 50)
    print(f"Entries WITH MAINTAINER: {len(entries_with_maintainer):,} unique combinations")
    print(f"Entries WITHOUT MAINTAINER: {len(entries_without_maintainer):,} unique Image IDs")
    print(f"Final dictionary size: {len(final_dict):,} entries")
    
    return final_dict, entries_with_maintainer, entries_without_maintainer

def display_results(final_dict, entries_with_maintainer, show_count=10):
    """
    Display the results in a readable format
    
    Args:
        final_dict (dict): Final dictionary with Image ID -> MAINTAINER mappings
        entries_with_maintainer (list): List of tuples (image_id, maintainer)
        show_count (int): Number of entries to display (default: 10)
    """
    print(f"\nFINAL DICTIONARY FORMAT (Image ID -> MAINTAINER):")
    print("=" * 50)
    print("Sample entries from the dictionary:")
    for i, (image_id, maintainer) in enumerate(list(final_dict.items())):
        print(f"  '{image_id}': '{maintainer}'")

def main():
    """
    Main function to process the CSV file and display results
    """
    # Path to the CSV file
    csv_file_path = './v2-report.csv'
    
    # Process the CSV file
    # For testing, you can add max_rows parameter, e.g., max_rows=50000
    unique_entries = process_csv_file(csv_file_path)
    
    if unique_entries:
        # Create final dictionary
        final_dict, entries_with_maintainer, entries_without_maintainer = create_final_dictionary(unique_entries)
        
        # Display results
        display_results(final_dict, entries_with_maintainer)
        
        # Print final dictionary variable info
        print(f"\nThe final dictionary 'final_dict' contains {len(final_dict):,} unique Image ID -> MAINTAINER mappings")
        print("You can access individual entries like: final_dict['<image_id>']")
        
        return final_dict
    else:
        print("No data was processed successfully.")
        return None

if __name__ == "__main__":
    # Run the main function
    result_dictionary = main()
