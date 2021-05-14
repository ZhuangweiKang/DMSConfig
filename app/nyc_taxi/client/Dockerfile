FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y python3-pip iproute2
RUN mkdir app
COPY * app/
WORKDIR /app
RUN pip3 install -r requirements.txt
RUN chmod +x *
CMD bash