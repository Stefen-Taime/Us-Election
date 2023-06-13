.PHONY: docker_up to_mongodb ingest_pyspark_1 ingest_pyspark_2

# Step 1: Run docker-compose
docker_up:
	docker-compose up -d

# Step 2: Run to-mongodb.py script
to_mongodb:
	cd src && python to-mongodb.py

# Step 3: Run process_1.py script
ingest_pyspark_1:
	docker exec -it $$(docker ps | grep us-election_spark_1 | awk '{print $$1}') /opt/bitnami/spark/bin/spark-submit --master local[*] --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /src/process_1.py

# Step 4: Run process_4.py script
ingest_pyspark_2:
	docker exec -it $$(docker ps | grep us-election_spark_1 | awk '{print $$1}') /opt/bitnami/spark/bin/spark-submit --master local[*] --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /src/process_4.py
