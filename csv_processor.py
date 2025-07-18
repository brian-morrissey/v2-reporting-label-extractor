import csv
import json
import time
from collections import defaultdict

def process_csv_file(csv_file_path, max_rows=None):
    """
    Process CSV file to extract unique Image ID and Namespace Labels/tenable.vsad pairs
    
    Args:
        csv_file_path (str): Path to the CSV file
        max_rows (int, optional): Maximum number of rows to process (None for all rows)
    
    Returns:
        dict: Dictionary with unique Image ID -> tenable.vsad mappings
    """
    unique_entries = {}
    missing_vsad_count = 0
    total_rows = 0
    vsad_found_count = 0
    
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
                
                # Extract Image ID and Namespace Labels
                image_id = row.get('Image ID', '') or ''
                namespace_labels_str = row.get('Namespace Labels', '') or ''
                
                # Strip whitespace if not None
                image_id = image_id.strip() if image_id else ''
                namespace_labels_str = namespace_labels_str.strip() if namespace_labels_str else ''
                
                # Skip rows with empty Image ID
                if not image_id:
                    continue
                
                # Parse Namespace Labels JSON
                vsad = None
                if namespace_labels_str:
                    try:
                        namespace_labels = json.loads(namespace_labels_str)
                        # Look for tenable.vsad key
                        for key, value in namespace_labels.items():
                            if key == 'kubernetes.namespace.label.tenable.vsad':
                                vsad = value
                                vsad_found_count += 1
                                break
                    except json.JSONDecodeError:
                        # If JSON parsing fails, skip this entry
                        continue
                
                # Count entries without vsad
                if vsad is None:
                    missing_vsad_count += 1
                
                # Create unique key combination (image_id, vsad)
                key = (image_id, vsad)
                if key not in unique_entries:
                    unique_entries[key] = {
                        'image_id': image_id,
                        'vsad': vsad
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
    print(f"Rows with vsad: {vsad_found_count:,}")
    
    return unique_entries

def create_final_dictionary(unique_entries):
    """
    Create final dictionary with Image ID -> vsad mappings
    
    Args:
        unique_entries (dict): Dictionary from process_csv_file function
    
    Returns:
        dict: Final dictionary with Image ID -> vsad mappings
    """
    # Separate entries with and without vsad
    entries_with_vsad = []
    entries_without_vsad = []
    
    for key, value in unique_entries.items():
        image_id, vsad = key
        if vsad:
            entries_with_vsad.append((image_id, vsad))
        else:
            entries_without_vsad.append((image_id, vsad))
    
    # Create final dictionary with only entries that have vsad
    final_dict = {}
    for image_id, vsad in entries_with_vsad:
        final_dict[image_id] = vsad
    
    return final_dict, entries_with_vsad, entries_without_vsad

def display_results(final_dict, entries_with_vsad, show_count=10):
    """
    Display the results in a readable format
    
    Args:
        final_dict (dict): Final dictionary with Image ID -> vsad mappings
        entries_with_vsad (list): List of tuples (image_id, vsad)
        show_count (int): Number of entries to display (default: 10)
    """
    print(f"\nFINAL DICTIONARY FORMAT (Image ID -> vsad):")
    print("=" * 50)
    print("Sample entries from the dictionary:")
    for i, (image_id, vsad) in enumerate(list(final_dict.items())):
        print(f"  '{image_id}': '{vsad}'")

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
        final_dict, entries_with_vsad, entries_without_vsad = create_final_dictionary(unique_entries)
        
        # Display results
        #display_results(final_dict, entries_with_vsad)
        
        # Print final dictionary variable info
        print(f"\nThe final dictionary 'final_dict' contains {len(final_dict):,} unique Image ID -> vsad mappings")
        #print("You can access individual entries like: final_dict['<image_id>']")
        
        # Write results to output.csv
        output_file = 'output.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Image ID', 'vsad'])
            for image_id, vsad in final_dict.items():
             writer.writerow([image_id, vsad])
        print(f"\nResults written to {output_file}")
        return final_dict
    else:
        print("No data was processed")
        return None

if __name__ == "__main__":
    # Run the main function
    result_dictionary = main()
