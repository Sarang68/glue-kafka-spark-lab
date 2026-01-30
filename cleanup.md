# Stop Kafka
docker-compose down

# Remove local files
rm -rf large_transactions.json pipeline_data.json output/ pipeline_output/

# Delete AWS resources (optional)
aws glue delete-job --job-name transactions-etl-job
aws glue delete-crawler --crawler transactions-crawler
aws glue delete-database --name transactions_db
aws s3 rm s3://your-glue-lab-bucket --recursive
