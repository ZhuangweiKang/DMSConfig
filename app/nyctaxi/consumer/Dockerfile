FROM ubuntu:18.04
# install jdk
RUN apt-get update
RUN apt-get install -y wget tar openjdk-8-jdk-headless -qq > /dev/null
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# install spark
RUN wget -q https://mirrors.gigenet.com/apache/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
RUN tar xf spark-2.4.7-bin-hadoop2.7.tgz >/dev/null
ENV SPARK_HOME="/spark-2.4.7-bin-hadoop2.7"
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 pyspark-shell"

RUN mkdir app
COPY * app/
WORKDIR /app

# install pyspark
RUN apt-get install -y python python-pip
RUN pip install -r requirements.txt
RUN chmod +x *
CMD bash