/*
This Scala script uses Apache Spark Structured Streaming to process and analyze news articles from a Kafka topic named `group5final`. 
It begins by defining a schema for the news articles and reads streaming data from Kafka. 
The script parses the JSON messages, flattens the nested structure, and converts the `publishedAt` field to a timestamp for further processing. 
It applies watermarking to handle late data and performs aggregation to count articles by source in 5-minute windows. 
The results are displayed in the console and saved to HDFS in Parquet format, with checkpointing enabled to manage the state of the streaming queries.
*/

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Define the schema for the news articles
val newsArticleSchema = new StructType(Array(
  StructField("author", StringType, true),
  StructField("content", StringType, true),
  StructField("description", StringType, true),
  StructField("publishedAt", StringType, true),
  StructField("source", StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true)
  )), true),
  StructField("title", StringType, true),
  StructField("url", StringType, true),
  StructField("urlToImage", StringType, true)
))

// Read stream from Kafka
val news = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "group5final")
  .load()

// Extract the JSON from the Kafka message and convert to DataFrame
val newsJsonDF = news.selectExpr("CAST(value AS STRING)").as[String]
val articlesDF0 = newsJsonDF.select(from_json($"value", newsArticleSchema).as("data")).select("data.*")

// Flatten the nested structure
val articlesDF = articlesDF0.select(
  col("author"),
  col("content"),
  col("description"),
  col("publishedAt"),
  col("source.id").alias("source_id"),
  col("source.name").alias("source_name"),
  col("title"),
  col("url"),
  col("urlToImage")
)

// Writing the results to the console
val query0 = articlesDF.writeStream
  .outputMode("append")
  .format("console")
  .start()

// Convert 'publishedAt' to timestamp for watermarking and aggregation
val articlesWithTimestampDF = articlesDF.withColumn("publishedAt", to_timestamp($"publishedAt", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

// Adding watermark to handle late data and perform aggregation
val articlesWithWatermarkDF = articlesWithTimestampDF
    .withWatermark("publishedAt", "10 minutes")
    .groupBy(
        window($"publishedAt", "5 minutes"),
        $"source_id"
    )
    .agg(count("*").alias("article_count"))

// Writing the aggregated results to the console
val query = articlesWithWatermarkDF.writeStream
  .outputMode("append")
  .format("console")
  .option("truncate", false) // Ensure that full content is displayed
  .start()

// Writing the aggregated results to HDFS in Parquet format
val parquet = articlesWithWatermarkDF.writeStream
  .format("parquet")
  .option("checkpointLocation", "file:///home/baralmona1/ckpt/parquet")
  .outputMode("append")
  .option("path", "hdfs://10.128.0.3:8020/project/article.parquet")
  .start()
