"""
Raygun Data Analysis
2024-11-07 | CR

pip install pandas matplotlib

"""
import os
import sys
import json
from datetime import datetime

from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt

DEBUG = False

load_dotenv()

# Path to the directory containing JSON files
# json_directory = '/path/to/json/files'
json_directory = os.environ.get('JSON_DIRECTORY')

# account_code = '61F2A207-EBB2-41C3-979C-FC98BE14DDA4'
account_code = os.environ.get('ACCOUNT_CODE')

print("")
print("-" * 20)
print("Raygun Data Analysis")
print("-" * 20)
print("")
print(f"Account code: {account_code}")
print(f"JSON directory: {json_directory}")
print("")
print("Processing... Press Ctrl+C to stop.")
print("")

# List to store extracted data
data = []
processed_count = 0

# Iterate through each JSON file and extract data
for filename in os.listdir(json_directory):
    _ = DEBUG and print(f"Processing file: {filename}")  # Debug log
    if filename.endswith('.json'):
        processed_count += 1
        filepath = os.path.join(json_directory, filename)
        if DEBUG:
            print(f"Opening file: {filepath}")  # Debug log
        else:
            # Progress indicator (spinner)
            sys.stdout.write(f"\rProcessing file: {filename}")
            sys.stdout.flush()

        with open(filepath, 'r') as file:
            try:
                content = json.load(file)
                _ = DEBUG and print(
                    f"File {filename} loaded successfully.")  # Debug log
                # Extract relevant data
                occurred_on = content.get('OccurredOn')
                user_custom_data_attr = content.get('UserCustomData', {})
                if not user_custom_data_attr:
                    _ = DEBUG and print(
                        f"Invalid user custom data attributes: "
                        f"{user_custom_data_attr}")  # Debug log
                    table_name = None
                else:
                    table_name = user_custom_data_attr.get('tableName')
                request_attr = content.get('Request', {})
                query_string_attr = request_attr.get('QueryString', {})
                if not query_string_attr:
                    _ = DEBUG and print(
                        "Invalid query string attributes: "
                        f"{query_string_attr}")  # Debug log
                    lac = None
                else:
                    lac = query_string_attr.get('lac')
                _ = DEBUG and print(
                        f"Extracted occurred_on: {occurred_on}, "
                        f"table_name: {table_name}, lac: {lac}")  # Debug log
                if occurred_on and lac == account_code:
                    # Convert occurred_on to a datetime object
                    dt = datetime.fromisoformat(
                        occurred_on.replace('Z', '+00:00'))
                    data.append({'date': dt.date(), 'table_name': table_name})
                    _ = DEBUG and print(
                        f"Appended data for {filename}")  # Debug log
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                print(f"Error processing {filename}: {e}")

sys.stdout.write("\rProcessing file: DONE")
sys.stdout.flush()

print("")
if DEBUG:
    print("")
    print("-" * 80)
    print("Error incidents for Account Code:", account_code)  # Debug log
    print("-" * 80)

# Min and max date
min_date = min(d['date'] for d in data)
max_date = max(d['date'] for d in data)
if DEBUG:
    print(f"Min date: {min_date}")
    print(f"Max date: {max_date}")

# Create a DataFrame from the data
print("")
print(f"Total files processed: {processed_count}")
print(f"Total records extracted: {len(data)}")

df = pd.DataFrame(data)

# Group by date and count occurrences
if DEBUG:
    print("")
    print("Grouping data by date...")  # Debug log
df_grouped = df.groupby('date').size().reset_index(name='error_count')
if DEBUG:
    print("")
    print("Data grouped successfully:")  # Debug log
    print(df_grouped.head())  # Debug log

# Display the report
if DEBUG:
    print("")
    print("Report by date and error count:")  # Debug log
    print("")
    # Partial report...
    print(df_grouped)

# ///////////////////////////////////////////

# Sort by date in descending order
df_grouped_by_date = df_grouped.sort_values(by='date', ascending=False)

# Display the fullreport
print("")
print("-" * 80)
print("Error incidents for Account Code:", account_code)
print("-" * 80)
print(f"From: {min_date}")
print(f"To:   {max_date}")
print("")
print("REPORT BY DATE AND ERROR COUNT")
print("(ordered by date in descending order)")
print("")
total = 0
print("Date\tError Count")
for index, row in df_grouped_by_date.iterrows():
    print(f"{row['date']}\t{row['error_count']}")
    total += row['error_count']
print("")
print(f"Total Error Count: {total}")

print("")
print("..............")

# Group by table and count occurrences
df_grouped_by_table = df.groupby('table_name') \
    .size() \
    .reset_index(name='error_count')
# Display the report
print("")
print("Report by table and error count:")  # Debug log
print("")
print(df_grouped_by_table)

print("")
print("..............")

# ///////////////////////////////////////////

# Plot the data
print("")
print("Plotting data...")  # Debug log
plt.figure(figsize=(12, 6))
plt.plot(df_grouped['date'], df_grouped['error_count'], marker='o')
plt.title(f'Daily Error Count for LAC {account_code}')
plt.xlabel('Date')
plt.ylabel('Error Count')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Analyzing trends
print("")
print("Descriptive statistics for grouped data:")  # Debug log
print(df_grouped.describe())
print("")
