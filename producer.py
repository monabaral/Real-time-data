"""
This Python script continuously fetches news articles related to 'Nepal' from specified sources using the News API, processes and filters the articles to remove placeholder fields, and sends the cleaned articles to a Kafka topic named 'group5final'.
The script initializes a Kafka producer for this purpose and schedules the process to repeat every 15 minutes, ensuring that new articles are regularly fetched, cleaned, and transmitted for further use or analysis.
"""

from newsapi import NewsApiClient
import json
from kafka import KafkaProducer
import time

# API key for News API
key = "d0a4bae2b31746deabe1c15fa41a17a0"

# Initialize News API client
newsapi = NewsApiClient(api_key=key)

# Define the list of media sources to fetch news from
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'

# Keep the producer running every hour
while True:
    # Fetch all articles from selected sources related to 'us' and in English
    all_articles = newsapi.get_everything(q='nepal',
                                          sources=sources,
                                          language='en')

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Iterate over fetched articles
    for article in all_articles['articles']:
        # Filter out articles with '[Removed]' fields (part of data cleaning)
        if article['title'] == '[Removed]' or article['author'] == '[Removed]' or article['publishedAt'] == '[Removed]':
            continue

        # Extract required fields from the article (part of data cleaning)
        filtered_article = {
            'author': article.get('author'),
            'content': article.get('content'),
            'description': article.get('description'),
            'publishedAt': article.get('publishedAt'),
            'source': {
                'id': article.get('source', {}).get('id'),
                'name': article.get('source', {}).get('name')
            },
            'title': article.get('title'),
            'url': article.get('url'),
            'urlToImage': article.get('urlToImage')
        }

        # Print the article before sending to Kafka
        print(f"Sending article to Kafka: {filtered_article}")

        # Send filtered article to Kafka topic 'group5final'
        producer.send('group5final', filtered_article)

    # Flush all messages in the producer buffer
    producer.flush()

    # Print status of articles sent to Kafka
    print("Sent filtered articles to Kafka.")

    print("waiting for 15 minutes before next fetch.")
    time.sleep(900)