import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Could not create keyspace due to {e}")


def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT);
        """)
        logging.info("Table created successfully!")
    except Exception as e:
        logging.error(f"Could not create table due to {e}")


def insert_data(session, **kwargs):
    logging.info("Inserting data...")
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (kwargs['id'], kwargs['first_name'], kwargs['last_name'], kwargs['gender'], kwargs['address'],
              kwargs['post_code'], kwargs['email'], kwargs['username'], kwargs['dob'],
              kwargs['registered_date'], kwargs['phone'], kwargs['picture']))
        logging.info(f"Data inserted for {kwargs['first_name']} {kwargs['last_name']}")
    except Exception as e:
        logging.error(f'Could not insert data due to {e}')


def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")
        return None


def connect_to_kafka(spark):
    import os
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.kafka:kafka-clients:3.4.1'

    try:
        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    try:
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        logging.debug("Schema applied to DataFrame successfully")
        return sel
    except Exception as e:
        logging.error(f"Could not create selection DataFrame from Kafka due to {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    spark_conn = create_spark_connection()

    if spark_conn:
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            selection_df = create_selection_df_from_kafka(kafka_df)
            session = create_cassandra_connection()
            if session:
                create_keyspace(session)
                create_table(session)
                # insert_data(session)
                # logging.info("Streaming is being started...")
                # streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                #                    .option('checkpointLocation', '/tmp/checkpoint')
                #                    .option('keyspace', 'spark_streams')
                #                    .option('table', 'created_users')
                #                    .start())
                # streaming_query.awaitTermination()
