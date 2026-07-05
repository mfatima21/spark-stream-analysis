

import json
import csv

import pandas as pd
import os
import glob
# Set the chunk size (number of lines per chunk)
chunk_size = 1000000
json_file_path = "/home/ubuntu/data/yelp_academic_dataset_review.json"
csv_file_path = "/home/ubuntu/data/"
dtype_mapping = {
    "review_id": "string",
    "user_id": "string",
    "business_id": "string",
    "stars": "float",
    "useful": "float",
    "funny": "float",
    "cool": "float",
    "text": "string",
    "date": "date"
}


def convert_json_to_csv(json_data, output_file):
    df = pd.DataFrame(json_data)
    df.to_csv(output_file, index=False)

def process_json_chunks():
       # Create a CSV writer object
    csv_writer = pd.DataFrame.to_csv
    
    # Read the JSON file in chunks
    json_reader = pd.read_json(json_file_path, encoding='utf-8', lines=True, chunksize=chunk_size, dtype=dtype_mapping)
    
    # Process each chunk and write to CSV
    chunk_count = 0
    for chunk in json_reader:

        print("about to read chunk number", chunk_count)

        chunk['text'] = chunk['text'].str.replace('\n', '')
        chunk['text'] = chunk['text'].str.replace("'", '')
        chunk['text'] = chunk['text'].str.replace(",", '')
        chunk['text'] = chunk['text'].str.replace('"', '')
        output_chunk = csv_file_path + 'chunk_{}.csv'.format(chunk_count)
        chunk.to_csv(output_chunk, encoding='utf-8', index=False, quoting=csv.QUOTE_NONNUMERIC)

        
        chunk_count += 1
        # if (chunk_count == 1):
        #     break

# Call the function to process JSON chunks and convert them to CSV


# Set the path to the directory containing the CSV files
# csv_directory = '/path/to/csv/files/'

# Set the path to the output consolidated CSV file
output_csv_file = '/home/ubuntu/data/mycsv/review.csv'

def combine_csv_files():
    all_files = glob.glob(os.path.join(csv_file_path, '*.csv'))
    i = 0
    # Concatenate all CSV files into a single DataFrame
    df_list = []
    for file in all_files:
        print("trying to combine file number", i)
        df = pd.read_csv(file)
        df_list.append(df)
        i = i + 1
        # if i > 3:
        #     break
    
    print("putside for loop trying to concat")
    combined_df = pd.concat(df_list, ignore_index=True)
    print("pcombined df to csv")
    # Save the consolidated DataFrame as a CSV file
    combined_df.to_csv(output_csv_file, index=False)

# Call the function to combine all CSV files
# process_json_chunks()
combine_csv_files()
