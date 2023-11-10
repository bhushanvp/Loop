from confluent_kafka import Consumer
from pyspark.sql import SparkSession

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "console-consumer-68511"
}
consumer = Consumer(config)

spark = SparkSession.builder \
                    .appName("Loop") \
                    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/loop_report_db.store, mongodb://127.0.0.1/loop_report_db/business_hours, mongodb://127.0.0.1/loop_report_db/timezones") \
                    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
                    .getOrCreate()

stores_df = spark.read \
                .option("collection", "store") \
                .format("com.mongodb.spark.sql.connector.MongoTableProvider") \
                .load()

business_hours_df = spark.read \
                .option("collection", "business_hours") \
                .format("com.mongodb.spark.sql.connector.MongoTableProvider") \
                .load()

timezones_df = spark.read \
                .option("collection", "timezones") \
                .format("com.mongodb.spark.sql.connector.MongoTableProvider") \
                .load()

def generate_report(reportname):
    report = {...}


    store_report_locally(report, reportname)

def store_report_locally(report, report_filename):
    with open(report_filename, 'w') as file:
        file.write(report)

consumer.subscribe(["report_generation_topic"])

try:
    while True:
        msg = consumer.poll(timeout=1)

        if msg is None:
            print("...")

        elif msg.error():
            print(msg.error())

        else:
            topic = msg.topic()
            partition = msg.partition()
            key = msg.key().decode('utf-8')
            value = msg.value().decode('utf-8')

            print("Recv -> ", topic, " : ", partition, " : ", key, " -> ", value)
            
            # generate_report(key)

            stores_df.printSchema()
            business_hours_df.printSchema()
            timezones_df.printSchema()

            
except KeyboardInterrupt:
    print("Shutting down...")
    consumer.close()