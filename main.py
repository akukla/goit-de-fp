from dataclasses import dataclass
import os
import logging
from typing import Dict, List, Optional
from enum import Enum
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError
from kafka.admin.new_topic import NewTopic
from colorama import Fore, Style, init
from pyspark.sql import SparkSession
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "pyspark-shell"
        )

findspark.init()

# Initialize colorama
init(autoreset=True)

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

mysql_jar_path = "/home/alex/PycharmProjects/goit-de-final/mysql-connector-j-8.0.32.jar"

# Configure logging with a custom formatter
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'INFO': Fore.GREEN,
        'DEBUG': Fore.BLUE
    }

    def format(self, record):
        if record.levelname in self.COLORS:
            record.msg = f"{self.COLORS[record.levelname]}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


# Set up logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class SecurityProtocol(Enum):
    PLAINTEXT = "PLAINTEXT"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SSL = "SSL"
    SASL_SSL = "SASL_SSL"


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    # username: str
    # password: str
    # security_protocol: SecurityProtocol
    # sasl_mechanism: str
    topic_prefix: str
    num_partitions: int = 2
    replication_factor: int = 1

    # def sasl_jaas_config(self) -> str:
    #     return (
    #         "org.apache.kafka.common.security.plain.PlainLoginModule required "
    #         f'username="{self.username}" password="{self.password}";'
    #     )

    @classmethod
    def from_env(cls) -> 'KafkaConfig':
        """Create configuration from environment variables with validation."""
        try:
            security_protocol = SecurityProtocol(os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"))
        except ValueError as e:
            raise ValueError(f"Invalid security protocol: {e}")
        return cls(
            bootstrap_servers=['localhost:9092'],
            topic_prefix=os.getenv("KAFKA_TOPIC_PREFIX", "akukla"),
            num_partitions=int(os.getenv("KAFKA_NUM_PARTITIONS", "2")),
            replication_factor=int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
        )

    def to_admin_config(self) -> Dict[str, str]:
        """Convert config to format expected by KafkaAdminClient."""
        # sasl_plain_str = f'PLAIN\n{self.username}\n{self.password}'
        return {
            "bootstrap_servers": self.bootstrap_servers,
            # "security_protocol": self.security_protocol.value,
            # "sasl_mechanism": self.sasl_mechanism,
            # "sasl_plain_username": self.username,
            # "sasl_plain_password": self.password,
        }


class KafkaAdminManager:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.admin_client = KafkaAdminClient(**config.to_admin_config())

    def close(self):
        """Close the admin client connection."""
        if self.admin_client:
            self.admin_client.close()

    def delete_topics(self, topics: List[str]) -> None:
        """Deletes Kafka topics with error handling."""
        try:
            existing_topics = self.admin_client.list_topics()
            topics_to_delete = [topic for topic in topics if topic in existing_topics]

            if topics_to_delete:
                self.admin_client.delete_topics(topics_to_delete)
                for topic in topics_to_delete:
                    logger.info(f"Topic '{topic}' deleted successfully")
            else:
                logger.info("No existing topics to delete")

        except KafkaError as ke:
            logger.error(f"Kafka error during topic deletion: {ke}")
        except Exception as e:
            logger.error(f"Unexpected error during topic deletion: {e}")

    def create_topics(self, topics_config: Dict[str, str]) -> None:
        """Creates Kafka topics with error handling."""
        try:
            new_topics = [
                NewTopic(
                    name=topic,
                    num_partitions=self.config.num_partitions,
                    replication_factor=self.config.replication_factor
                )
                for topic in topics_config
            ]

            self.admin_client.create_topics(new_topics)
            for topic in topics_config:
                logger.info(f"Topic '{topic}' created successfully")

        except KafkaError as ke:
            logger.error(f"Kafka error during topic creation: {ke}")
        except Exception as e:
            logger.error(f"Unexpected error during topic creation: {e}")

    def list_topics(self) -> Optional[List[str]]:
        """Lists existing Kafka topics that match the prefix."""
        try:
            all_topics = self.admin_client.list_topics()
            matching_topics = [
                topic for topic in all_topics
                if self.config.topic_prefix in topic
            ]

            for topic in matching_topics:
                logger.info(f"Found existing topic: '{topic}'")
            return matching_topics

        except KafkaError as ke:
            logger.error(f"Kafka error during topic listing: {ke}")
        except Exception as e:
            logger.error(f"Unexpected error during topic listing: {e}")
        return None


def create_topics():
    """Main execution function."""
    admin_manager = None
    try:
        # Load configuration
        config = KafkaConfig.from_env()

        # Define topics
        topics_to_create = {
            f"{config.topic_prefix}_athlete_event_results": "Topic for athlete event results",
            f"{config.topic_prefix}_enriched_athlete_avg": "Topic for enriched athlete averages"
        }

        # Initialize admin manager
        admin_manager = KafkaAdminManager(config)

        logger.info("Starting Kafka admin operations...")

        # Execute operations
        admin_manager.delete_topics(list(topics_to_create.keys()))
        admin_manager.create_topics(topics_to_create)
        admin_manager.list_topics()

    except Exception as e:
        logger.error(f"Failed to execute Kafka admin operations: {e}")
        raise
    finally:
        if admin_manager:
            admin_manager.close()

def read_and_send_athlete_event_results_data():

    mysql_properties = {
        "user": jdbc_user,
        "password": jdbc_password,
        "driver": "com.mysql.jdbc.Driver"
    }
    # Create and configure Spark session
    spark = (
        SparkSession.builder
        .config("spark.jars", mysql_jar_path)
        .config("spark.driver.extraClassPath", mysql_jar_path)
        .config("spark.executor.extraClassPath", mysql_jar_path)
        .config("spark.sql.streaming.checkpointLocation", 'checkpoints')
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.driver.memory", "2g")  # Increased memory for better performance
        .config("spark.executor.memory", "2g")
        .appName("EnhancedJDBCToKafka")
        .master("local[*]")
        .getOrCreate()
    )

    # Verify MySQL driver is loaded
    try:
        spark.sparkContext._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
        logger.info("MySQL driver 8.3.0 successfully loaded")
    except Exception as e:
        logger.error(f"Failed to load MySQL driver: {e}")
        raise

    df = (spark.read.format("jdbc").options(
                    url=jdbc_url,
                    driver="com.mysql.cj.jdbc.Driver",
                    dbtable=jdbc_table,
                    user=jdbc_user,
                    password=jdbc_password,
                    partitionColumn="athlete_id",
                    lowerBound=1,
                    upperBound=1000000,
                    numPartitions=10
                ).load())
    df.show()

    try:
        kafka_config = KafkaConfig.from_env()
        topic = 'akukla_athlete_event_results'
        (df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
         .write
         .format("kafka")
         .option("kafka.bootstrap.servers", ",".join(kafka_config.bootstrap_servers))
         .option("topic", topic)
         .save())
    except Exception as e:
        logger.error(f"Error writing to Kafka topic {topic}: {str(e)}")
        raise


def read_kafka_topic():
    kafka_config = KafkaConfig.from_env()
    topic = 'akukla_athlete_event_results'

    # Create and configure Spark session
    spark = (
        SparkSession.builder
        .appName("ReadKafkaTopic")
        .master("local[*]")
        .getOrCreate()
    )

    schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", IntegerType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", BooleanType(), True)
    ])

    # Read from Kafka topic
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config.bootstrap_servers))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
        .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.athlete_id", "data.sport", "data.medal")
    )

    df_bio = (spark.read.format("jdbc").options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable='athlete_bio',
        user=jdbc_user,
        password=jdbc_password,
        partitionColumn="athlete_id",
        lowerBound=1,
        upperBound=1000000,
        numPartitions=10
    ).load())

    df_bio_filtered = df_bio.filter(
        (col("height").isNotNull()) & (~isnan(col("height"))) &
        (col("weight").isNotNull()) & (~isnan(col("weight")))
    )

    df_bio_filtered.show()

    batch_df = (stream_df.join(df_bio_filtered, "athlete_id")
     .groupBy("sport", "medal", "sex", "country_noc")
     .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
     )
    )

    def foreach_batch_function(batch_df: DataFrame, epoch_id: int) -> None:
        try:
            # Write to Kafka
            topic = 'akukla_enriched_athlete_avg'
            try:
                kafka_config = KafkaConfig.from_env()
                (batch_df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
                 .write
                 .format("kafka")
                 .option("kafka.bootstrap.servers", ",".join(kafka_config.bootstrap_servers))
                 .option("topic", topic)
                 .save())
            except Exception as e:
                logger.error(f"Error writing to Kafka topic {topic}: {str(e)}")
                raise

            # Write to MySQL
            (batch_df.write
             .format("jdbc")
             .options(
                url="jdbc:mysql://217.61.57.46:3306/neo_data",
                driver="com.mysql.cj.jdbc.Driver",
                dbtable=f"akukla_enriched_athlete_avg",
                user=jdbc_user,
                password=jdbc_password,
            )
             .mode("append")
             .save())

            logger.info(f"Batch processed successfully for epoch {epoch_id}.")
        except Exception as e:
            logger.error(f"Error in batch processing (epoch {epoch_id}): {str(e)}")
            raise


    (batch_df.writeStream
     .outputMode("complete")  # Use 'complete' or 'append' depending on requirements
     .format("console")
     .option("truncate", "false")  # Full output without truncation
     .option("numRows", 50)  # Number of rows to display
     .start())

    print('Console output:')
    (batch_df.writeStream
     .outputMode("complete")
     .foreachBatch(foreach_batch_function)
     .option("checkpointLocation", os.path.join('checkpoints', "streaming"))
     .start()
     .awaitTermination())

    # Select the value column and cast it to string
    # df = df.selectExpr("CAST(value AS STRING)")

    # Write the stream to the console
    # query = (
    #     df.writeStream
    #     .outputMode("append")
    #     .format("console")
    #     .option("truncate", "false")
    #     .start()
    # )
    #
    # query.awaitTermination()

if __name__ == "__main__":
    # create_topics()
    read_and_send_athlete_event_results_data()
    read_kafka_topic()