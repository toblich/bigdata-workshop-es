import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    StructType,
    StructField,
    TimestampType,
)


def get_args(valid_queries):
    parser = argparse.ArgumentParser(description="Streaming practice")
    parser.add_argument(
        "--brokers", nargs="+", help="Kafka brokers (host:port)", required=True
    )
    parser.add_argument(
        "--topics", nargs="+", help="Kafka topics", required=True
    )
    parser.add_argument(
        "--query", choices=valid_queries, type=int, help="Query to run", required=True
    )
    parser.add_argument(
        "--timeout", type=int, help="Timeout for the query", default=None
    )
    return parser.parse_args()


def create_spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def start_stream(brokers, topics, query_function, query_timeout):

    spark = create_spark_session(f"Stocks:Stream:ETL | Query:{query_fn}")

    json = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", ",".join(brokers))
        .option("subscribe", ",".join(topics))
        .load()
    )

    json.printSchema()

    # Explicitly set schema
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("price", DoubleType(), False),
    ])

    json_options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm'Z'"}
    stocks_json = json.select(
        F.from_json(F.col("value").cast("string"), schema, json_options).alias("content")
    )

    stocks_json.printSchema()

    stocks = stocks_json.select("content.*")

    query = query_function(stocks)
    print("You can stop the execution at any time with CTRL + C")
    query.awaitTermination(timeout=query_timeout)
    query.stop()


def define_write_to_postgres(table_name):

    def write_to_postgres(df, epochId):
        print(f"Bacth (epochId): {epochId}")
        return (
            df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres/workshop")
            .option("dbtable", f"workshop.{table_name}")
            .option("user", "workshop")
            .option("password", "w0rkzh0p")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
    return write_to_postgres


def summarize_stocks(stocks):
    avg_pricing = (
        stocks
        .withWatermark("timestamp", "60 seconds")
        .groupBy(
            F.window("timestamp", "30 seconds"),
            stocks.symbol,
        )
        .agg(F.avg("price").alias('avg_price'))
    )
    avg_pricing.printSchema()
    return avg_pricing


# Query 1
def get_query_parquet_output(df):
    output_path = "/dataset/streaming.parquet"
    checkpoint_path = "/dataset/checkpoint"

    transformed_df = (
        df
        .withColumn("year", F.year(F.col("timestamp")))
        .withColumn("month", F.month(F.col("timestamp")))
        .withColumn("day", F.dayofmonth(F.col("timestamp")))
        .withColumn("hour", F.hour(F.col("timestamp")))
        .withColumn("minute", F.minute(F.col("timestamp")))
    )

    query = (
        transformed_df.writeStream
        .format("parquet")
        .partitionBy("year", "month", "day", "hour", "minute")
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("The query check every 30 seconds for new data.")
    print(f"Output path: {output_path}")
    print(f"Checkpoint path: {checkpoint_path}")

    return query


# Query 2
def get_query_console_output_avg_price(df):
    transformed_df = (
        df
        .groupBy(F.col("symbol"))
        .agg(F.avg(F.col("price")).alias("avg_price"))
    )

    query = (
        transformed_df.writeStream
        .outputMode("complete")
        .format("console")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("The query check every 10 seconds for new data.")

    return query


# Query 3
def get_query_postgre_output(df):
    table_name = "streaming_inserts"

    transformed_df = (
        df
        .withWatermark("timestamp", "60 seconds")
        .select("timestamp", "symbol", "price")
    )

    foreach_batch_writer = define_write_to_postgres(table_name)

    query = (
        transformed_df
        .writeStream
        .foreachBatch(foreach_batch_writer)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("The query check every 10 seconds for new data.")
    print(f"Table name: {table_name}")

    return query


# Query 4
def get_query_postgre_output_avg_price(df):
    table_name = "streaming_inserts_avg_price"

    transformed_df = summarize_stocks(df)

    window_to_string = F.udf(lambda w: str(w.start) + " - " + str(w.end), StringType())

    foreach_batch_writer = define_write_to_postgres(table_name)

    query = (
        transformed_df
        # .withColumn("window", F.concat_ws(" - ", "window.start", "window.end"))
        .withColumn("window", window_to_string("window"))
        .writeStream
        .foreachBatch(foreach_batch_writer)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("The query check every 10 seconds for new data.")
    print(f"Table name: {table_name}")

    return query


# Query 5
def get_query_postgre_output_avg_price_windows(df):
    table_name = "streaming_inserts_avg_price_final"

    transformed_df = summarize_stocks(df)

    foreach_batch_writer = define_write_to_postgres(table_name)

    query = (
        transformed_df
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .writeStream
        .foreachBatch(foreach_batch_writer)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("The query check every 10 seconds for new data.")
    print(f"Table name: {table_name}")

    return query


if __name__ == "__main__":
    queries = {
        1: get_query_parquet_output,
        2: get_query_console_output_avg_price,
        3: get_query_postgre_output,
        4: get_query_postgre_output_avg_price,
        5: get_query_postgre_output_avg_price_windows,
    }

    args = get_args(valid_queries=list(queries))

    query_fn = queries[args.query]
    
    start_stream(
        brokers=args.brokers,
        topics=args.topics,
        query_function=query_fn,
        query_timeout=args.timeout
    )
