mvn package
$SPARK_HOME/bin/spark-submit --class gr.unipi.datacron.JoinApp target/DRDF_Query-1.0-SNAPSHOT-jar-with-dependencies.jar input/queries/q.ini
