# Test ijson parsing
python -c "import ijson; print('ijson OK')"

# Test Kafka connection
python -c "from kafka import KafkaProducer; p = KafkaProducer(bootstrap_servers=['localhost:9092']); print('Kafka OK')"

# Test PySpark
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); print('Spark OK'); spark.stop()"

# Test AWS credentials
aws sts get-caller-identity

# List Kafka topics
docker exec -it $(docker ps -q -f name=kafka) kafka-topics --list --bootstrap-server localhost:9092
