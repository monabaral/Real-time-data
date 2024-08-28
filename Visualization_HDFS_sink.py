"""
This Python script connects to an HDFS server to load Parquet files containing article data into a pandas DataFrame.
It then continuously updates a bar chart every 120 seconds to visualize the count of articles by source.
If no data is available, the script displays a message indicating the absence of data. The visualization updates in real-time, ensuring the latest information is presented.
"""
import time
import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow.hdfs as hdfs

# HDFS directory where the Parquet files are stored
PARQUET_DIR = '/project/article.parquet'
fs = hdfs.connect('10.128.0.3', 8020)


def load_data():
    # Connect to HDFS

    # List files in the HDFS directory
    files = fs.ls(PARQUET_DIR)

    # print(files)

    data = []
    for file_path in files:
        if file_path.endswith('.parquet'):
            # Load each Parquet file into a DataFrame
            with fs.open(file_path) as f:
                df = pq.read_table(f).to_pandas()
                data.append(df)

    if data:
        # Concatenate all dataframes into a single dataframe
        return pd.concat(data, ignore_index=True)
    return pd.DataFrame()


df = load_data()
df.info()

# visualization

while True:
    # Function to update the plot
    plt.clf()  # Clear the plot
    df = load_data()
    plt.figure(figsize=(12, 8))
    if not df.empty:
        # Example visualization: Count of articles by source
        counts = df['source_id'].value_counts()
        ax = counts.plot(kind='bar', color='skyblue')

        # Display count values on bars
        for p in ax.patches:
            height = p.get_height()
            ax.text(p.get_x() + p.get_width() / 2., height, int(height),
                    ha='center', va='bottom', fontsize=10, color='black')

        plt.title('Count of Articles by Source')
        plt.xlabel('Source ID')
        plt.ylabel('Number of Articles')
    else:
        plt.text(0.5, 0.5, 'No data available', fontsize=12, ha='center')
    plt.tight_layout()
    plt.show()
    time.sleep(120)
