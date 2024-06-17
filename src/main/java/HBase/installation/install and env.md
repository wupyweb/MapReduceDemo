# 1. 单机版安装
**相关知识**

在安装HBase之前你需要先安装Hadoop和Zookeeper，如果你还没有安装可以通过这两个实训来学习：[Hadoop安装与配置](https://www.educoder.net/shixuns/wliam3nq/challenges)，[Zookeeper安装与配置](https://www.educoder.net/shixuns/ienafu69/challenges)。
本次实训的环境已经默认安装好了Hadoop，接下来我们就开始安装配置HBase吧。

HBase安装
HBase的安装也分为三种，单机版、伪分布式、分布式；我们先来安装单机版。

首先我们去官网下载好HBase的安装包；

接下来，将压缩包解压缩到你想安装的目录（安装包在平台已经下载好了，在/opt目录下，无需你再进行下载，我们统一将HBase解压到/app目录下）：

```shell
mkdir /app
cd /opt
tar -zxvf hbase-2.1.1-bin.tar.gz -C /app
```
安装单机版很简单，我们只需要配置JDK的路径即可，我们将JDK的路径配置到conf/下的hbase-env.sh中。

我们先输入echo $JAVA_HOME来复制JAVA_HOME的路径，以方便之后的配置：



接着我们编辑HBaseconf目录下的hbase-env.sh文件，将其中的JAVA_HOME指向到你Java的安装目录，最后保存设置：
```shell
export JAVA_HOME=XXX
```


然后编辑hbase-site.xml文件，在<configuration>标签中添加如下内容：

```xml
<configuration>
  <property>
       <name>hbase.rootdir</name>
       <value>file:///root/data/hbase/data</value>
  </property>
  <property>
       <name>hbase.zookeeper.property.dataDir</name>
       <value>/root/data/hbase/zookeeper</value>
  </property>
  <property>     
  <name>hbase.unsafe.stream.capability.enforce</name>
        <value>false</value>
  </property>
</configuration>
```
以上各参数的解释：

- hbase.rootdir：这个目录是region server的共享目录，用来持久化Hbase。URL需要是'完全正确'的，还要包含文件系统的scheme。例如，要表示hdfs中的 /hbase目录，namenode 运行在namenode.example.org的9090端口。则需要设置为hdfs://namenode.example.org:9000 /hbase。默认情况下Hbase是写到/tmp的。不改这个配置，数据会在重启的时候丢失
- hbase.zookeeper.property.dataDir：ZooKeeper的zoo.conf中的配置。快照的存储位置，默认是: ${hbase.tmp.dir}/zookeeper
- hbase.unsafe.stream.capability.enforce：控制HBase是否检查流功能（hflush / hsync），如果您打算在rootdir表示的LocalFileSystem上运行，那就禁用此选项。
配置好了之后我们就可以启动HBase了，在启动之前我们可以将Hbase的bin目录配置到/etc/profile中，这样更方便我们以后操作。
在etc/profile的文件末尾添加如下内容：

```shell
HBASE_HOME=/app/hbase-2.1.1
export PATH=$PATH:$HBASE_HOME/bin
```
HBASE_HOME为你自己本机Hbase的地址。

不要忘了，source /etc/profile使刚刚的配置生效。

接下来我们就可以运行HBase来初步的体验它的功能了：

首先需要启动Hadoop，输入命令start-dfs.sh来启动Hadoop，输入jps查看是否启动成功，接着我们输入start-hbase.sh来启动HBase，同样输入jps查看是否启动成功，出现了HMaster即表示启动成功了。

# 2. 伪分布式环境搭建
上次实训中我们已经完成了单机版HBase的安装，单机版意味着我们的HBase数据仍然是存放在本地，而没有存放在Hadoop集群中，本关我们来学习如何配置一个伪分布式的HBase环境，伪分布式意味着HBase仍然在单个主机上运行，但每个HBase的守护程序（HMaster、HRegionServer和Zookeeper）作为单独的进程运行；在伪分布式的环境下，我们会将HBase的数据存储在HDFS中，而不是存放在本地了，接下来我们就来一起搭建环境吧。

实验环境：

- hadoop2.7；
- JDK8；
- HBase2.1.1；

hadoop已安装；
JDK已安装，环境变量已配置；
HBase压缩包已下载，存放在/opt目录下。

在搭建环境之前我们首先来了解一下HBase分布式环境的整体架构：


我们来简单认识一下与HBase的相关组件：
![](https://data.educoder.net/api/attachments/dFFSY1lWc2MwVFE4QWVUYUEvcDdzUT09)

**Zookeeper**：

Zookeeper能为HBase提供协同服务，是HBase的一个重要组件，Zookeeper能实时的监控HBase的健康状态，并作出相应处理。

**HMaster**：

HMaster是HBase的主服务，他负责监控集群中所有的HRegionServer，并对表和Region进行管理操作，比如创建表，修改表，移除表等等。

**HRegion**：

HRegion是对表进行划分的基本单元，一个表在刚刚创建时只有一个Region，但是随着记录的增加，表会变得越来越大，HRegionServer会实时跟踪Region的大小，当Region增大到某个值时，就会进行切割（split）操作，由一个Region切分成两个Region。

**HRegionServer**：

HRegionServer是RegionServer的实例，它负责服务和管理多个HRegion 实例，并直接响应用户的读写请求。

总的来说，要部署一个分布式的HBase数据库，需要各个组件的协作，HBase通过Zookeeper进行分布式应用管理，Zookeeper相当于管理员，HBase将数据存储在HDFS（分布式文件系统）中，通过HDFS存储数据，所以我们搭建分布式的HBase数据库的整体思路也在这里，即将各个服务进行整合。

接下来，我们就一起来搭建一个伪分布式的HBase。

配置与启动伪分布式HBase
如果你已经完成了单节点HBase的安装，那伪分布式的配置对你来说应该很简单了，只需要修改hbase-site.xml文件即可：

`vim /app/hbase-2.1.1/conf/hbase-site.xml`
在这里主要有两项配置：

1.开启HBase的分布式运行模式，配置hbase.cluster.distributed为true代表开启HBase的分布式运行模式：

2.是设置HBase的数据文件存储位置为HDFS的/hbase目录，要注意的是在这里我们不需要在HDFS中手动创建hbase目录，因为HBase会帮我们自动创建。

修改之后hbase-site.xml的<configuration>代码：
```xml
<configuration>
  <property>
       <name>hbase.rootdir</name>
       <value>hdfs://localhost:9000/hbase</value>
  </property>
  <property>
       <name>hbase.zookeeper.property.dataDir</name>
       <value>/root/data/hbase/zookeeper</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
</configuration>
```

标红部分为我们需要注意的配置。我们在设置单节点的时候将hbase.unsafe.stream.capability.enforce属性值设置为了false，在这里我们需要注意设置它的值为true，或者干脆删除这个属性也是可以的。

配置完成之后，我们需要先启动Hadoop，命令为：start-dfs.sh，然后启动HBase，最后输入jps查看启动的进程：


如果出现HMaster和HRegionServer以及HQuorumPeer三个服务则代表伪分布式环境已经搭建成功了。

在HDFS中验证
接下来我们进一步验证：在HDFS中检查HBase文件。

如果一切正常，HBase会在HDFS中自动建立自己的文件，在上述配置文件中，设置的文件位置为/hbase，我们输入hadoop fs -ls /hbase即可查看，如下图所示，分布式文件系统（HDFS）中hbase文件夹已经创建了：