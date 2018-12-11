FROM ubuntu:16.04

ENV RMQ_HOSTNAME=two.radical-project.org
ENV RMQ_PORT=33247
ENV RADICAL_PILOT_DBURL="mongodb://user:user123@ds223653.mlab.com:23653/bioexcel-1"

RUN apt-get update \
    && apt-get install wget curl python python-dev python-pip python-virtualenv bzip2 -y \
    && virtualenv ~/entk \
    && . ~/entk/bin/activate \
    && pip install radical.entk

VOLUME [ "/output" ]
COPY runme.py /

CMD . ~/entk/bin/activate && python runme.py && cp ./output_* /output/