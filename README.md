This is big data tech task by Illya Piven.

Tools that were used here were:

Spark 2.0.2
Java 1.8.0_111
Kafka 2.11-0.10.1.0
Cassandra 3.0.9
Datastax 3.1.0
The main problem of the project is the fact that it never ran as a whole because of only 1GB RAM available on my virtual machine. To run Cassandra separately I've used a swap file, but unexpectedly, it wasn't enough to run the whole application. Just in case I'm adding screenshots that were made without Cassandra on different stages.

To run the whole task I've used the following command: spark-submit —total-executor-cores 3 —executor-memory 512M —master spark://appliance:7077 —class com.kadylo.app.App jWork/TechTask/target/TechTask-1.0-SNAPSHOT.jar