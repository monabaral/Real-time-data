"""
This Python script reads JSON files from a specified local directory, loads their contents into pandas DataFrames, and concatenates these into a single DataFrame.
It then continuously updates a bar chart every 120 seconds to visualize the count of articles by author.
If no data is available, it displays a message indicating that.
The script clears the plot, updates the DataFrame with the latest data, and refreshes the visualization in real-time.
"""
import time
import matplotlib.pyplot as plt
import os
import pandas as pd
import json

# Local directory where the JSON files are stored
JSON_DIR = '/home/baralmona1/project/article.json'


def load_data():
    # List files in the local directory
    files = os.listdir(JSON_DIR)
    data = []
    for file_name in files:
        if file_name.endswith('.json'):
            file_path = os.path.join(JSON_DIR, file_name)
            try:
                # Load each JSON file into a DataFrame
                with open(file_path) as f:
                    file_data = json.load(f)
                    df = pd.json_normalize(file_data)
                    data.append(df)
            except json.JSONDecodeError as e:
                print(f" ")
            except Exception as e:
                print(f" ")

    if data:
        # Concatenate all dataframes into a single dataframe
        return pd.concat(data, ignore_index=True)
    return pd.DataFrame()


data = load_data()

print(data.head())  # Print the first few rows of the data

# visualization

while True:
    # Function to update the plot
    plt.clf()  # Clear the plot
    df = load_data()
    plt.figure(figsize=(12, 8))
    if not df.empty:
        # Example visualization: Count of articles by source
        counts = df['author'].value_counts()
        ax = counts.plot(kind='bar', color='skyblue')

        # Display count values on bars
        for p in ax.patches:
            height = p.get_height()
            ax.text(p.get_x() + p.get_width() / 2., height, int(height),
                    ha='center', va='bottom', fontsize=10, color='black')

        plt.title('Count of Articles by Author')
        plt.xlabel('Author')
        plt.ylabel('Number of Articles')
    else:
        plt.text(0.5, 0.5, 'No data available', fontsize=12, ha='center')
    plt.tight_layout()
    plt.show()
    time.sleep(120)
