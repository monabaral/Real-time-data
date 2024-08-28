/*
This Scala script uses Apache Spark Structured Streaming to process news articles from Kafka. 
It starts by defining a schema for the articles and creating a `SparkSession`. 
The script reads streaming data from a Kafka topic named `group5final`, extracts and flattens the JSON data, and converts the `publishedAt` field to a timestamp for processing. 
It applies watermarking to handle late data and aggregates the data by author in 5-minute windows. 
The script writes the original article data and the aggregated results to the console and saves the aggregated results as JSON files to a specified local directory, with checkpointing enabled to manage the state of streaming queries.
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



// Create a SparkSession
val spark = SparkSession.builder
  .appName("Author Article Count Stream")
  .master("local[*]")
  .getOrCreate()
  
import spark.implicits._


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

// Writing the data to the console
val consoleQuery1 = articlesDF.writeStream
  .outputMode("append")
  .format("console")
  .start()

// Convert 'publishedAt' to timestamp for watermarking and aggregation
val articlesWithTimestampDF = articlesDF.withColumn("publishedAt", to_timestamp($"publishedAt", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

// Adding watermark to handle late data
val articlesWithWatermarkDF = articlesWithTimestampDF
  .withWatermark("publishedAt", "10 minutes")


// Aggregating based on author
val aggregatedByAuthorDF = articlesWithWatermarkDF
  .groupBy(
    $"author",
    window($"publishedAt", "5 minutes")
  )
  .agg(
    count("*").alias("article_count_by_author")
  )

// Writing the aggregated results by author to the console
val consoleQuery2 = aggregatedByAuthorDF.writeStream
  .outputMode("append")
  .format("console")
  .option("truncate", false) // Ensure that full content is displayed
  .start()

// Writing the aggregated results by author to local HDFS in json format
val parquetJson2 = aggregatedByAuthorDF.writeStream
  .format("json")
  .option("checkpointLocation", "file:///home/baralmona1/ckpt/json")
  .outputMode("append")
  .option("path", "file:///home/baralmona1/project/article.json")
  .start()
