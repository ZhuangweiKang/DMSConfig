FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y python3-pip
RUN mkdir app
COPY * app/
WORKDIR /app
RUN pip3 install -r requiresments.txt
RUN chmod +x *
CMD bash