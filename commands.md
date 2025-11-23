# KAFKA
### Run zookeeper server
<Path_to>/kafka_2.13-3.9.0/bin/zookeeper-server-start.sh <Path_to>/kafka_2.13-3.9.0/config/zookeeper.properties
(Kafka requires ZooKeeper to manage the brokers. Ensure ZooKeeper is running before starting Kafka)

### Run Kafka broker (server)
<Path_to>/kafka_2.13-3.9.0/bin/kafka-server-start.sh <Path_to>/kafka_2.13-3.9.0/config/server.properties
(Make sure server.properties is properly configured (e.g., broker.id, log.dirs, listeners))

### Create Kafka Topic (iot-data)
kafka-topics.sh --create --topic iot-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
(🔹 --partitions 3: Splits data across 3 partitions.
🔹 --replication-factor 1: Only one copy of the data (not fault-tolerant, ideally should be 2 or more).)

### list all topics
kafka-topics.sh --list --bootstrap-server localhost:9092
(This will display all available topics in the Kafka cluster.)

### Kafka Console Consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-data --from-beginning
(This will display all messages sent to the topic from the beginning.)

### Kafka Console Producer
kafka-console-producer.sh --broker-list localhost:9092 --topic iot-data
(After running this command, you can type messages, and they will be sent to the topic.)

### Check Kafka logs if you encounter issues:
cat logs/server.log

### Monitor Kafka topics and messages:
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-data --from-beginning --property print.key=true --property print.timestamp=true


## Docker-based Kafka CLI – Quick Guide
### Enter Kafka container
docker compose exec kafka bash

### or run commands directly
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

### Topic Management
#### List topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

#### Create topic
docker compose exec kafka kafka-topics \
  --create --topic my_topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

#### Delete topic
docker compose exec kafka kafka-topics \
  --delete --topic my_topic \
  --bootstrap-server localhost:9092

#### Describe topic
docker compose exec kafka kafka-topics \
  --describe --topic my_topic \
  --bootstrap-server localhost:9092

### Producer & Consumer
#### Start Producer
docker compose exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic my_topic

#### Start Consumer
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my_topic --from-beginning

### Consumer Groups
#### List groups
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list

#### Describe group
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group my_group

#### Broker Info
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

### If using container name directly
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list


# AIRFLOW
### Airflow Setup
airflow db init – Initialize the metadata database
airflow db upgrade – Apply DB migrations
airflow users create – Create an Airflow user
airflow users list

### Start Services
airflow webserver – Start Web UI
airflow scheduler – Start Scheduler
airflow triggerer – Start Triggerer (for deferrable tasks)

### DAG Management
airflow dags list – List all DAGs
airflow dags show <dag_id> – Show DAG structure
airflow dags trigger <dag_id> – Trigger a DAG manually
airflow dags pause <dag_id> – Pause DAG
airflow dags unpause <dag_id> – Unpause DAG

### Task Commands
airflow tasks list <dag_id> – List tasks in a DAG
airflow tasks test <dag_id> <task_id> <date> – Test task without scheduler
airflow tasks clear <dag_id> – Clear task instances

### Variables & Connections
airflow variables list
airflow variables set KEY VALUE
airflow connections list

### Status & Info
airflow info – Show Airflow environment info
airflow version – Show version

## Docker-based Airflow CLI – Quick Guide
## Enter Airflow container
docker compose exec airflow-webserver bash

### Then run normal Airflow CLI:
airflow dags list
airflow tasks list my_dag
airflow users list

### Run CLI directly (no shell)
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags trigger my_dag
docker compose exec airflow-webserver airflow tasks test my_dag task1 2025-01-01

## Common Docker Airflow Commands
### DAGs
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags pause my_dag
docker compose exec airflow-webserver airflow dags unpause my_dag
docker compose exec airflow-webserver airflow dags trigger my_dag

### Tasks
docker compose exec airflow-webserver airflow tasks list my_dag
docker compose exec airflow-webserver airflow tasks clear my_dag

### Users
docker compose exec airflow-webserver airflow users list
docker compose exec airflow-webserver airflow users create

### DB
docker compose exec airflow-webserver airflow db init
docker compose exec airflow-webserver airflow db upgrade

### If using container name (not compose)
docker exec -it airflow-webserver airflow dags list


# MINIO
## Docker-based MinIO CLI – Quick Guide
MinIO is managed via mc (MinIO Client).

### Access MinIO container
docker compose exec minio bash

### Or list containers to confirm name:
docker ps

### Configure MinIO alias
mc alias set local http://localhost:9000 minioadmin minioadmin
(replace credentials if custom)

### Bucket Management
#### List buckets
mc ls local

#### Create bucket
mc mb local/my-bucket

#### Remove bucket
mc rb local/my-bucket

### File Operations
#### Upload file
mc cp file.csv local/my-bucket

#### Download file
mc cp local/my-bucket/file.csv ./file.csv

#### List bucket content
mc ls local/my-bucket

### Access & Policies
#### Set public policy
mc anonymous set public local/my-bucket

#### Set private
mc anonymous set none local/my-bucket

### Useful Admin Commands
#### Disk usage
mc du local/my-bucket

#### Mirror directory to bucket
mc mirror ./data local/my-bucket

### Run without shell (direct)
docker compose exec minio mc ls local
docker compose exec minio mc mb local/test-bucket