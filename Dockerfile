FROM debian:11

ENV LC_ALL C.UTF-8

# init
ADD ./release/ /root/
RUN bash /root/init_debian.sh && rm -rf /root/init_debian.sh

# code
RUN cd /root/ && tar -xzvf FilterKnn.tar.gz

WORKDIR /root

VOLUME ["/data"]

EXPOSE 8898

CMD ["bash", "/root/run.sh"]
