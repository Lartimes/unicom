FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/tubaoshu/jdk-17.0.6:1.0

# 本地hadoop目录，也可以FROM linux系统，网络下载且解压
COPY ./hadoop/ /opt/hadoop
COPY ./activation-1.1.jar /opt/hadoop/share/hadoop/common/
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/usr/local/jdk-17.0.6
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=${PATH}:/opt/hadoop/bin:/opt/hadoop/sbin:/usr/local/jdk-17.0.6/bin



COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE  9000 9870 9864 8088 19888 8030 8031 8032 8033 8040 8042

CMD ["bash"]
