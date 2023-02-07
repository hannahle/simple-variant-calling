FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:6839-main

RUN apt-get update && apt-get install -y curl unzip git

RUN apt-get update -y &&\
    apt-get install -y autoconf samtools
RUN apt-get install bcftools
# RUN apt install bwa

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.

RUN pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
