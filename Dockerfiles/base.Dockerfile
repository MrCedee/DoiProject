ARG debian_buster_image_tag=8-jre-slim
FROM openjdk:${debian_buster_image_tag}

# -- Layer: OS + Python 3.7

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p ${shared_workspace}/data && \
    mkdir -p ${shared_workspace}/notebooks && \
    touch ${shared_workspace}/notebooks/ini.ipynb

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

CMD ["bash"]