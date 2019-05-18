FROM ubuntu
USER root

RUN apt-get -y update &&\
    apt-get -y install git python-virtualenv python-dev librocksdb-dev python3-pip\
                       build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev\
                       liblz4-dev locales

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
    pip3 install -r requirements.txt &&\
    pip3 install connexion[swagger-ui]

COPY docker-run.sh /root/run.sh
RUN chmod +x /root/run.sh

ENTRYPOINT /root/run.sh