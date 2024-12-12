#!/bin/bash

#scp /home/lsc/spark/core/target/spark-core_2.11-2.4.3.jar 192.168.200.20:/home/lsc/hadoop/spark-2.4.3/jars
#scp /home/lsc/spark/core/target/spark-core_2.11-2.4.3.jar 192.168.200.21:/home/lsc/hadoop/spark-2.4.3/jars
#scp /home/lsc/spark/core/target/spark-core_2.11-2.4.3.jar 192.168.203.20:/home/lsc/hadoop/spark-2.4.3/jars
#scp /home/lsc/spark/core/target/spark-core_2.11-2.4.3.jar 192.168.203.21:/home/lsc/hadoop/spark-2.4.3/jars
#scp /home/lsc/spark/core/target/spark-core_2.11-2.4.3.jar 192.168.200.14:/home/lsc/hadoop/spark-2.4.3/jars

sshpass ssh lsc@192.168.200.20 "/home/lsc/hadoop/spark-2.4.3/sbin/stop-slave.sh"
sshpass ssh lsc@192.168.200.21 "/home/lsc/hadoop/spark-2.4.3/sbin/stop-slave.sh"
sshpass ssh lsc@192.168.203.20 "/home/lsc/hadoop/spark-2.4.3/sbin/stop-slave.sh"
sshpass ssh lsc@192.168.203.21 "/home/lsc/hadoop/spark-2.4.3/sbin/stop-slave.sh"
sshpass ssh lsc@192.168.200.14 "/home/lsc/hadoop/spark-2.4.3/sbin/stop-master.sh"
sshpass ssh lsc@192.168.200.14 "/home/lsc/hadoop/spark-2.4.3/sbin/start-master.sh"
sshpass ssh lsc@192.168.200.20 "/home/lsc/hadoop/spark-2.4.3/sbin/start-slave.sh -c 16 -m 32g -d /home/lsc/hadoop/worker spark://192.168.200.14:7077"
sshpass ssh lsc@192.168.200.21 "/home/lsc/hadoop/spark-2.4.3/sbin/start-slave.sh -c 16 -m 32g -d /home/lsc/hadoop/worker spark://192.168.200.14:7077"
sshpass ssh lsc@192.168.203.20 "/home/lsc/hadoop/spark-2.4.3/sbin/start-slave.sh -c 8 -m 16g -d /home/lsc/hadoop/worker spark://192.168.200.14:7077"
sshpass ssh lsc@192.168.203.21 "/home/lsc/hadoop/spark-2.4.3/sbin/start-slave.sh -c 8 -m 16g -d /home/lsc/hadoop/worker spark://192.168.200.14:7077"

