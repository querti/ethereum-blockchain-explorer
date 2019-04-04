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

RUN cd /home &&\
    git clone https://github.com/querti/ethereum-blockchain-explorer.git &&\
    cd /home/ethereum-blockchain-explorer &&\
    git checkout cursed-branch &&\
    pip3 install -r requirements.txt

ADD run.sh /root/run.sh
RUN chmod +x /root/run.sh

ENTRYPOINT /root/run.sh