#!/bin/bash
/home/mpcs53013/hadoop-2.7.1/sbin/start-dfs.sh
/home/mpcs53013/hadoop-2.7.1/sbin/start-yarn.sh
/home/mpcs53013/zookeeper-3.4.6/bin/zkServer.sh start /home/mpcs53013/zookeeper-3.4.6/conf/zoo.cfg
sleep 60
/home/mpcs53013/hbase-1.1.2/bin/start-hbase.sh
sleep 60
/home/mpcs53013/hbase-1.1.2/bin/hbase-daemon.sh start rest
/home/mpcs53013/kafka_2.10-0.9.0.1/bin/kafka-server-start.sh -daemon /home/mpcs53013/kafka_2.10-0.9.0.1/config/server.properties
nohup /home/mpcs53013/apache-storm-0.10.0/bin/storm nimbus &
nohup /home/mpcs53013/apache-storm-0.10.0/bin/storm supervisor &
nohup /home/mpcs53013/apache-storm-0.10.0/bin/storm ui &
