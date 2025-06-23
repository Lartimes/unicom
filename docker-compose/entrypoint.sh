#!/bin/bash
set -e

# 创建必要的目录
mkdir -p ${HADOOP_CONF_DIR} ${HADOOP_HOME}/data /hadoop/dfs/{name,data} /hadoop/journaldata

# 生成 core-site.xml
cat > ${HADOOP_CONF_DIR}/core-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${FS_DEFAULTFS}</value>
  </property>
  <property>
    <name>hadoop.http.staticuser.user</name>
    <value>${HADOOP_HTTP_STATICUSER_USER}</value>
  </property>
  <property>
    <name>ha.zookeeper.quorum</name>
    <value>${HA_ZOOKEEPER_QUORUM}</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>${DFS_PERMISSIONS_ENABLED}</value>
  </property>
</configuration>
EOF

# 生成 hdfs-site.xml
cat > ${HADOOP_CONF_DIR}/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>dfs.ha.fencing.methods</name>
  <value>sshfence</value>
</property>
  <!-- JournalNode相关配置 -->
  <property>
    <name>dfs.namenode.shared.edits.dir</name>
    <value>qjournal://zmd01:8485;zmd02:8485;zmd03:8485/mycluster</value>
  </property>
   <property>
    <name>dfs.journalnode.edits.dir</name>
    <value>/hadoop/journaldata</value>
  </property>
  <property>
    <name>dfs.nameservices</name>
    <value>${DFS_NAMESERVICES}</value>
  </property>
  <property>
    <name>dfs.ha.namenodes.${DFS_NAMESERVICES}</name>
    <value>${DFS_HA_NAMENODES_MYCLUSTER}</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address.${DFS_NAMESERVICES}.zmd01</name>
    <value>${DFS_NAMENODE_RPC_ADDRESS_MYCLUSTER_ZMD01}</value>
  </property>
  <property>
    <name>dfs.namenode.http-address.${DFS_NAMESERVICES}.zmd01</name>
    <value>${DFS_NAMENODE_HTTP_ADDRESS_MYCLUSTER_ZMD01}</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address.${DFS_NAMESERVICES}.zmd02</name>
    <value>${DFS_NAMENODE_RPC_ADDRESS_MYCLUSTER_ZMD02}</value>
  </property>
  <property>
    <name>dfs.namenode.http-address.${DFS_NAMESERVICES}.zmd02</name>
    <value>${DFS_NAMENODE_HTTP_ADDRESS_MYCLUSTER_ZMD02}</value>
  </property>
  <property>
    <name>dfs.ha.automatic-failover.enabled</name>
    <value>${DFS_HA_AUTOMATIC_FAILOVER_ENABLED}</value>
  </property>
  <property>
    <name>dfs.client.failover.proxy.provider.${DFS_NAMESERVICES}</name>
    <value>${DFS_CLIENT_FAILOVER_PROXY_PROVIDER_MYCLUSTER}</value>
  </property>
</configuration>
EOF

# 生成 yarn-site.xml
cat > ${HADOOP_CONF_DIR}/yarn-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
        <name>hadoop.zk.address</name>
        <value>${HA_ZOOKEEPER_QUORUM}</value>
  </property>
  <property>
        <name>yarn.resourcemanager.recovery.enabled</name>
        <value>true</value>
  </property>
  <property>
        <name>yarn.resourcemanager.store.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${YARN_RESOURCEMANAGER_HOSTNAME_RM1}</value>
  </property>
  <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>${YARN_RESOURCEMANAGER_HOSTNAME_RM1}:8088</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>


  <property>
    <name>yarn.resourcemanager.ha.enabled</name>
    <value>${YARN_RESOURCEMANAGER_HA_ENABLED}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.cluster-id</name>
    <value>${YARN_RESOURCEMANAGER_CLUSTERS}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.ha.rm-ids</name>
    <value>${YARN_RESOURCEMANAGER_HA_RM_IDS}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname.rm1</name>
    <value>${YARN_RESOURCEMANAGER_HOSTNAME_RM1}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname.rm2</name>
    <value>${YARN_RESOURCEMANAGER_HOSTNAME_RM2}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address.rm1</name>
    <value>${YARN_RESOURCEMANAGER_WEBAPP_ADDRESS_RM1}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address.rm2</name>
    <value>${YARN_RESOURCEMANAGER_WEBAPP_ADDRESS_RM2}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.zk-address</name>
    <value>${YARN_RESOURCEMANAGER_ZK_ADDRESS}</value>
  </property>
</configuration>
EOF

# 生成 mapred-site.xml
cat > ${HADOOP_CONF_DIR}/mapred-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.app.mapreduce.am.command-opts</name>
    <value>--add-opens java.base/java.lang=ALL-UNNAMED -Xmx1024m</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
</configuration>
EOF

echo "配置文件已成功生成到 ${HADOOP_CONF_DIR} 目录"
echo '172.30.0.2  mycluster' >> /etc/hosts 
echo '172.30.0.3  mycluster' >> /etc/hosts 

case "$NODE_ROLE" in
  "zhaomingdong01" | "zhaomingdong02" | "zhaomingdong03")
    # 启动JournalNode服务
    hdfs --daemon start journalnode
    echo "JournalNode started successfully"
    ;;
  *)
    echo "Unknown NODE_ROLE: $NODE_ROLE"
    exit 1
    ;;
esac


# 根据角色执行不同的启动逻辑
case "$NODE_ROLE" in
    zhaomingdong01)
        echo "启动主NameNode节点..."
        hdfs zkfc -formatZK -force
        hdfs --daemon start zkfc
        hdfs namenode -format
        hdfs --daemon start namenode
        hdfs --daemon start datanode
        yarn --daemon start resourcemanager
        yarn --daemon start nodemanager
        ;;
        
    zhaomingdong02)
        echo "启动备用NameNode节点..."
        hdfs --daemon start zkfc
        hdfs namenode -bootstrapStandby
        hdfs --daemon start namenode
        hdfs --daemon start datanode
        yarn --daemon start nodemanager
        ;;
        
    zhaomingdong03)
        sleep 10
        echo "启动DataNode和JournalNode节点..."
        hdfs --daemon start datanode
        yarn --daemon start nodemanager
        ;;
    *)
        echo "未知角色: $NODE_ROLE，仅启动DataNode和NodeManager..."
        hdfs --daemon start journalnode
        hdfs --daemon start datanode
        yarn --daemon start nodemanager
        ;;
esac


tail -f >> /dev/null