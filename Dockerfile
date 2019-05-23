FROM ubuntu:19.04
USER root

RUN apt-get -y update &&\
    apt-get -y install git python-virtualenv python-dev librocksdb-dev=5.17.2-3 python3-pip\
                       build-essential=12.6ubuntu1 libsnappy-dev=1.1.7-1\
                       zlib1g-dev=1:1.2.11.dfsg-1ubuntu2 libbz2-dev=1.0.6-9 libgflags-dev=2.2.2-1\
                       liblz4-dev=1.8.3-1ubuntu1 locales=2.29-0ubuntu2

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN mkdir /home/ethereum-blockchain-explorer
COPY ./src /home/ethereum-blockchain-explorer/src/
COPY ./cfg /home/ethereum-blockchain-explorer/cfg/
COPY ./tests /home/ethereum-blockchain-explorer/tests/
COPY ./main.py /home/ethereum-blockchain-explorer/
COPY ./requirements.txt /home/ethereum-blockchain-explorer/
COPY ./tox.ini /home/ethereum-blockchain-explorer/
RUN cd /home/ethereum-blockchain-explorer &&\
    pip3 install -r requirements.txt

COPY docker-run.sh /root/run.sh
RUN chmod +x /root/run.sh

ENTRYPOINT /root/run.sh