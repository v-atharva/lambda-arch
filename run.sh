#!/bin/bash

# Check if MySQL database is set up
echo "Checking if MySQL database 'proj4' exists..."
if mysql -u root -patharva -e "use proj4" 2>/dev/null; then
    echo "Database 'proj4' exists"
else
    echo "Creating database 'proj4'..."
    mysql -u root -patharva -e "CREATE DATABASE proj4"
    echo "Database created."
fi

# Initialize database with data
echo "Initializing database from CSV files..."
python src/init_db.py

# Ensure Kafka is running (this assumes Kafka is running locally)
echo "Checking if Kafka is running on localhost:9092..."
nc -z localhost 9092
if [ $? -ne 0 ]; then
    echo "ERROR: Kafka does not appear to be running on localhost:9092"
    echo "Please start Kafka before running this script."
    exit 1
fi

# Run producer
echo "Starting transaction producer..."
python src/stream_producer.py

# Run consumer
echo "Starting transaction consumer..."
python src/stream_consumer.py

# Run batch processor
echo "Starting batch processor..."
python src/batch_processor.py

echo "Processing complete! Results are available in the 'results' directory." 