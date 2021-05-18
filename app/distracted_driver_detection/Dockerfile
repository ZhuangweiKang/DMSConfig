FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y python3-pip iproute2 ffmpeg libsm6 libxext6
RUN mkdir app
COPY . app/
WORKDIR /app
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
RUN pip3 install tensorflow --upgrade --force-reinstall
RUN chmod +x *
CMD bash