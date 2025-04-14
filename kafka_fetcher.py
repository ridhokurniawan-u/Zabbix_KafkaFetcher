import time
import subprocess
from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition

psk_file = "/etc/zabbix/encrypt.psk"
psk_identity = "yuh"

group_id = "my-cool-group"
topic = "test-topic"
broker = "192.168.1.205:9092"

admin = KafkaAdminClient(bootstrap_servers=broker)
consumer = KafkaConsumer(group_id=group_id, bootstrap_servers=broker)

prev_offsets = {}
prev_time = time.time()

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

    total_lag = 0

    current_time = time.time()
    elapsed_time = current_time - prev_time

    message_throughput = 0
    
    for tp in tps:
        latest = consumer.position(tp)
        committed = consumer.committed(tp)
        total_lag += latest - (committed or 0)
        prev = prev_offsets.get(tp.partition, latest)
        message_throughput += max(latest - prev, 0)
        prev_offsets[tp.partition] = latest

    through_per_sec = message_throughput / elapsed_time if elapsed_time else 0

    print("Current Consumer Lag:", total_lag)
    time.sleep(25)

    message_throughput = 0

    zabbix_host = "192.168.1.249"  
    zabbix_item_key = f'kafka.lag[{group_id},{topic}]'
    zabbix_hostname = "Tester Client" 

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
    print("Zabbix Kafka Partition push result:", result.stdout.decode())

    result = subprocess.run(cmd_heartbeat, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("Zabbix Kafka Heartbeat push result:", result.stdout.decode())

    result = subprocess.run(cmd_health, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("Zabbix Kafka Health push result:", result.stdout.decode())

    result = subprocess.run(cmd_lag, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("Zabbix Kafka Lag push result:", result.stdout.decode())

    result = subprocess.run(cmd_throughput, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("Zabbix Kafka Throughput push result:", result.stdout.decode())

