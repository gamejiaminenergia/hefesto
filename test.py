import pandas as pd
import json
import os
from pathlib import Path

# Directory containing the JSON files
data_dir = Path('/home/aldelab/hefesto/test')

# List to store individual DataFrames
data_frames = []

# Iterate over all JSON files in the directory
for json_file in data_dir.glob('*.json'):
    try:
        # Read the JSON file
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert to DataFrame and add to the list
        # If the JSON is a list, it will create a row for each item
        # If it's a dictionary, it will create a single row
        df = pd.json_normalize(data)
        data_frames.append(df)
        print(f"Processed: {json_file.name}")
    except Exception as e:
        print(f"Error processing {json_file.name}: {str(e)}")

# Combine all DataFrames into one
if data_frames:
    combined_df = pd.concat(data_frames, ignore_index=True)
    
    # Display information about the combined DataFrame
    print("\nCombined DataFrame Info:")
    print(f"Total files processed: {len(data_frames)}")
    print(f"Total rows in combined DataFrame: {len(combined_df)}")
    print("\nFirst few rows of the combined DataFrame:")
    print(combined_df.head())
    
    # Optional: Save to a single CSV file
    output_file = 'combined_data.xlsx'
    combined_df.to_excel(output_file, index=False)
    print(f"\nCombined data saved to {output_file}")
    print(combined_df)
else:
    print("No valid JSON files found or processed.")