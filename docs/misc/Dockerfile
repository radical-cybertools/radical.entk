FROM ubuntu:16.04

ENV RMQ_HOSTNAME=two.radical-project.org
ENV RMQ_PORT=33247
ENV RADICAL_PILOT_DBURL="mongodb://user:user@ds247688.mlab.com:47688/entk-docs"

RUN apt-get update \
    && apt-get install wget curl python python-dev python-pip python-virtualenv bzip2 -y \
    && virtualenv ~/ve-entk \
    && . ~/ve-entk/bin/activate \
    && pip install radical.entk