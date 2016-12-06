# trending-topics-analysis
Distributed Java application that reads tweets and processes them with Apache Storm and Kafka

STORM_HOME="/home/Downloads/apache-storm-1.0.2"

export STORM_HOME

export PATH=$PATH:$STORM_HOME/bin

start zookeeper server: 

bin/zookeeper-server-start.sh config/zookeeper.properties

start kafka server:

bin/kafka-server-start.sh config/server.properties

create topic "Tweets":

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Tweets

list available topics:

bin/kafka-topics.sh --list --zookeeper localhost:2181

start consumer (standard output):

bin/kafka-console-consumer.sh -bootstrap-server localhost:9092 --topic Tweets --from-beginning

Start storm zookeeper

bin/zkServer.sh start stormZk.cfg

Connect to zookeeper Cli

bin/zkCli.sh -server IP:2181

Stop zookeeper

zkServer.sh stop stormZk.cfg

Start Nimbus Daemon

$STORMROOT/bin/storm nimbus

Start Supervisor Daemon

$STORMROOT/bin/storm supervisor

Start User Interface

$STORMROOT/bin/storm ui

Storm User interface address
http://localhost:8080/index.html