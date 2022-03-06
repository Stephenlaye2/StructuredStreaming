import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object SparkStreaming extends App {
  val conf = new SparkConf()
    .setAppName("SparkKafkaStreaming")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
val spark = SparkSession.builder()
  .master("local[2]")
  .config(conf)
  .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

//  Read Stream
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "capstone_tweet")
    .option("startingOffsets", "earliest")
    .load()

  df.printSchema()

  val stringDF = df.selectExpr("CAST(value AS STRING)").as[String]

  val dataSchema = new StructType()
    .add("id", LongType)
    .add("text", StringType)
    .add("created_at", StringType)
    .add("screen_name", StringType)
    .add("followers_count", IntegerType)
    .add("favourite_count", IntegerType)
    .add("retweet_count", IntegerType)

  val tweetDF = stringDF.withColumn("value", from_json(col("value"), dataSchema))
    .select(col("value.*"))

//  Write Stream
  tweetDF.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "true")
    .start()
    .awaitTermination()
}
