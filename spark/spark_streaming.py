import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType, StructType


def create_keyspace(session):
    # create keyspace cassandra DB
    session.execute("""
        CREATE KEYSPACE IF NOT EXIST spark_streams
        WITH replication = {"class": "SimpleStrategy", "replication_factor": "1"};
    """)
    logging.info("Keyspace created successfully")

def create_table(session):
    #create cassandras table here
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT
        )
    """)

#kwargs -> argumentos de longitud variable
def insert_data(session, **kwargs):
    logging.info("INSERTING DATA TO CASSANDRA...")
    #Mapping data

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')

    try:
        session.execute("""
         INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address)
         VALUES(%s, %s, %s, %s, %s)
        """),(user_id, first_name, last_name, gender, address)

        logging.info(f"DATA INSERTED FOR {first_name} {last_name}")
    except Exception as ex:
        logging.error(f"ERROR INSERTING DATA: {ex}")

def create_spark_connector():
    #stablish Spark connector
    spark_c = None
    try:
        spark_c = SparkSession.builder.appName("SparkDataStreaming")\
                    .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1")\
                    .config("spark.cassandra.connection.host", "localhost")\
                    .getOrCreate()
        spark_c.sparkContext.setLogLevel("ERROR")
        logging.info("SPARK session created success")
    except Exception as error:
        logging.error(f"Error to create SPARK session: {error}")
        return None
    return spark_c
def create_cassandra_connector():
    #cassandra connector cluster
    cas_session = None

    try:
        cluster_cass = Cluster(["localhost"])
        cas_session = cluster_cass.connect()
    except Exception as err:
        logging.error(f"Could not connect to cassandra DB connector: {err}")
        return None

    return cas_session

#-------KAFKA-------
def connect_kafka(spark_connector):
      #connecting kafka to spark
    spark_df = None

    try:
        spark_df = spark_connector.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("KAFKA DATAFRAME CREATED SUCCESSFULLY")
    except Exception as ex:
        logging.warning(f"kafka dataframe could not be created because: {ex}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False)
    ])

    selector = spark_df.selectExpr("CAST(value as STRING)") \
                .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(selector)

    return selector

if __name__ == "__main__":
    spark_connector = create_spark_connector()

    if spark_connector is not None:
        #connect kafka with spark
        spark_df = connect_kafka(spark_connector)
        session = create_cassandra_connector()
        selection_df = create_selection_df_from_kafka(spark_df)

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming data is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option("checkpointLocation", "/tmp/checkpoint")
                               .option("keyspace", "spark_streams")
                               .option("table", "created_users")
                               .start()
                            )
            streaming_query.awaitTermination()
