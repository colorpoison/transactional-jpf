FROM ubuntu:24.04

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get -y install openjdk-11-jre && \
    apt-get clean && \
    update-alternatives --config java && \
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 && \
    apt -y install git-all && \
    rm -rf /var/lib/apt/lists/*

RUN apt update && \
    apt -y upgrade && \
    apt -y install python3-pip && \
    apt -y install python3-venv

RUN apt -y install python3-pandas python3-numpy python3-matplotlib

RUN apt -y install nano

WORKDIR /trans-jpf

ADD . /trans-jpf/

RUN mkdir -p build && \
    cp lib/cloning-1.10.3.jar build && \
    ./gradlew buildJars

ENTRYPOINT [ "/bin/bash", "-c" ]
