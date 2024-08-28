"""
This Python script connects to an HDFS cluster to load Parquet files containing article data into a Pandas DataFrame.
It processes the data by grouping articles by source and summing the counts.
The script then converts this processed data into document format and generates embeddings using a pre-trained model from sentence-transformers.
It creates a FAISS index for efficient similarity search, sets up a Hugging Face GPT-2 model for text generation, and integrates it with LangChain for question-answering.
Finally, it tests the system by querying which source has the highest number of articles, leveraging the retrieval-augmented generation (RAG) model to find the answer.
"""

!pip install sentence-transformers
!pip install torch torchvision torchaudio
!pip install transformers
!pip install transformers langchain faiss-cpu
!pip install -U langchain-community
!pip install transformers --upgrade
import torch

import time
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
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


df1 = load_data()
df1.info()

from transformers import pipeline, AutoModelForCausalLM, AutoTokenizer
from langchain.chains import LLMChain, RetrievalQA
from langchain.chains.question_answering import load_qa_chain
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.schema import Document
from langchain.llms import HuggingFacePipeline

df = df1.groupby('source_id')['article_count'].sum().reset_index()

# Convert DataFrame to Documents
documents = [
    Document(
        page_content=f"Source: {row['source_id']}, Number of articles: {row['article_count']}",
        metadata={'source': row['source_id']}
    ) for index, row in df.iterrows()
]

# Generate embeddings
embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")

# Build the FAISS index
vector_store = FAISS.from_documents(documents, embeddings)

# Initialize retriever
retriever = vector_store.as_retriever()

# Load the language model
model_name = "gpt2"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name)

# Create a HuggingFace pipeline and wrap it for LangChain
hf_pipeline = pipeline("text-generation", model=model, tokenizer=tokenizer, max_new_tokens=50)
llm = HuggingFacePipeline(pipeline=hf_pipeline)

# Load a question-answering chain using the LLM
qa_chain = load_qa_chain(llm, chain_type="stuff")

# Create the RetrievalQA chain
retrieval_qa_chain = RetrievalQA(
    retriever=retriever,
    combine_documents_chain=qa_chain
)

# Test the QA pipeline
query = "Which source is having highest number of articles?"
result = retrieval_qa_chain({"query": query})
print(result["result"])
