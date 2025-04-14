import logging
import logging.config
import time
import subprocess
from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition

psk_file = "/muh/frickin/encrypt.psk"
psk_identity = "nuh uh"

group_id = "muh-kul-group"
topic = "test-topic"
broker = "muh broker ip:muh port"

admin = KafkaAdminClient(bootstrap_servers=broker)
consumer = KafkaConsumer(group_id=group_id, bootstrap_servers=broker)


prev_offsets = {}
prev_time = time.time()

logging.basicConfig(
    filename="/home/tester/kafka.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


while True:
    print("=" * 30)
    print("Fetching Kafka Metrics....")

    partitions = consumer.partitions_for_topic(topic)

    if partitions is None:
        print("Topic not found or broker down.")
        time.sleep(30)
        continue

    tps = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)

    # Kafka Consumer Lag
    total_lag = 0

    # Kafka Message Troughput
    current_time = time.time()
    elapsed_time = current_time - prev_time

    end_offsets = consumer.end_offsets(tps)

    message_throughput = 0

    for tp in tps:
        latest = end_offsets[tp]
        committed = consumer.committed(tp)
        total_lag += latest - (committed or 0)
        prev = prev_offsets.get(tp.partition, latest)
        message_throughput += max(latest - prev, 0)
        prev_offsets[tp.partition] = latest

        logging.info(f"Partition {tp.partition} | Committed: {committed} | Latest: {latest} | Lag: {latest - (committed or 0)}")

    through_per_sec = message_throughput / elapsed_time if elapsed_time else 0

    print("Current Consumer Lag:", total_lag)
    time.sleep(5)

    message_throughput = 0

    zabbix_host = "duh"
    zabbix_item_key = f'kafka.lag[{group_id},{topic}]'
    zabbix_hostname = "fit to dash"

    cmd_lag = [
        "zabbix_sender",
        "--tls-connect=psk",
        "--tls-psk-identity", psk_identity,
        "--tls-psk-file", psk_file,
        "-z", zabbix_host,
        "-s", zabbix_hostname,
        "-k", zabbix_item_key,
        "-o", str(total_lag)
    ]

    cmd_throughput = [
        "zabbix_sender",
        "--tls-connect=psk",
        "--tls-psk-identity", psk_identity,
        "--tls-psk-file", psk_file,
        "-z", zabbix_host,
        "-s", zabbix_hostname,
        "-k", f"kafka.topic.throughput[{topic}]",
        "-o", str(through_per_sec)
    ]

    cmd_heartbeat = [
        "zabbix_sender",
        "--tls-connect=psk",
        "--tls-psk-identity", psk_identity,
        "--tls-psk-file", psk_file,
        "-z", zabbix_host,
        "-s", zabbix_hostname,
        "-k", f"kafka.consumer.heartbeat[{group_id}]",
        "-o", "1"
    ]

    cmd_partition = [
        "zabbix_sender",
        "--tls-connect=psk",
        "--tls-psk-identity", psk_identity,
        "--tls-psk-file", psk_file,
        "-z", zabbix_host,
        "-s", zabbix_hostname,
        "-k", f"kafka.topic.partitions[{topic}]",
        "-o", str(len(partitions))
    ]

    try:
        KafkaAdminClient(bootstrap_servers=broker)
        broker_status = 1
    except:
        broker_status = 0

    cmd_health = [
        "zabbix_sender",
        "--tls-connect=psk",
        "--tls-psk-identity", psk_identity,
        "--tls-psk-file", psk_file,
        "-z", zabbix_host,
        "-s", zabbix_hostname,
        "-k", f"kafka.broker.status[{broker}]",
        "-o", str(broker_status)
    ]

    prev_time = time.time()

    result = subprocess.run(cmd_partition, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = subprocess.run(cmd_heartbeat, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = subprocess.run(cmd_health, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = subprocess.run(cmd_lag, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = subprocess.run(cmd_throughput, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
