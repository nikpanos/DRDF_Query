# mvn clean package
$SPARK_HOME/bin/spark-submit \
  --deploy-mode client \
  --class gr.unipi.datacron.App \
  target/DRDF_Query-1.0-SNAPSHOT-jar-with-dependencies.jar \
  queries/StarQuery.hocon
