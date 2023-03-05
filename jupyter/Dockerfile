FROM jupyter/pyspark-notebook

USER root

RUN wget --no-check-certificate https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.420/aws-java-sdk-1.12.420.jar -O /usr/local/spark-3.3.2-bin-hadoop3/jars/aws-java-sdk-1.12.420.jar && \
  wget --no-check-certificate https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O /usr/local/spark-3.3.2-bin-hadoop3/jars/hadoop-aws-3.3.4.jar && \
  wget --no-check-certificate https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar -O /usr/local/spark-3.3.2-bin-hadoop3/jars/jets3t-0.9.4.jar

RUN wget --no-check-certificate https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.420/aws-java-sdk-bundle-1.12.420.jar -O /usr/local/spark-3.3.2-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.420.jar

USER jovyan
