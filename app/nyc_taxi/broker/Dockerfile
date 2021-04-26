FROM ubuntu:18.04

RUN apt-get update && apt-get install -y default-jdk wget tar lsof
RUN apt install -y iproute2
RUN wget https://downloads.apache.org/kafka/2.7.0/kafka_2.13-2.7.0.tgz
RUN tar xzf kafka_2.13-2.7.0.tgz
RUN mv kafka_2.13-2.7.0 /kafka

WORKDIR /kafka
RUN rm -r site-docs
RUN rm -r bin/windows
CMD bash