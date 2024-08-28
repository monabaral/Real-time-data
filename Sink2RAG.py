"""
This Python script processes JSON files containing article data to answer queries about authors' article counts.
It begins by loading JSON files from a specified directory into a Pandas DataFrame.
The data is then aggregated by author to sum the number of articles each has written.
The script converts this aggregated data into documents, generates embeddings using a pre-trained model, and builds a FAISS index for efficient document retrieval.
It sets up a GPT-2 model with LangChain for question-answering, creating a RetrievalQA chain to handle queries.
Finally, the script tests the system by querying which author has the highest number of articles and prints the result.
"""

!pip install sentence-transformers
!pip install torch torchvision torchaudio
!pip install transformers
!pip install transformers langchain faiss-cpu
!pip install -U langchain-community
!pip install transformers --upgrade
import torch

import time
import matplotlib.pyplot as plt
import os
import pandas as pd
import json

# Local directory where the JSON files are stored
JSON_DIR = '/home/baralmona1/project/article.json'  # Replace with your actual path


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
                print(f"")
            except Exception as e:
                print(f"")

    if data:
        # Concatenate all dataframes into a single dataframe
        return pd.concat(data, ignore_index=True)
    return pd.DataFrame()


# Call load_data() to load the data from the local directory
data = load_data()

data.info()

from transformers import pipeline, AutoModelForCausalLM, AutoTokenizer
from langchain.chains import LLMChain, RetrievalQA
from langchain.chains.question_answering import load_qa_chain
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.schema import Document
from langchain.llms import HuggingFacePipeline

df = data.groupby('author')['article_count_by_author'].sum().reset_index()

# Convert DataFrame to Documents
documents = [
    Document(
        page_content=f"Author: {row['author']}, Number of articles: {row['article_count_by_author']}",
        metadata={'author': row['author']}
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
query = "Which author has the highest number of articles?"
result = retrieval_qa_chain({"query": query})
print(result["result"])
