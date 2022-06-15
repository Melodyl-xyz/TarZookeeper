# 介绍
TarZookeeper在读取zoo.cfg配置后，能够挑选出包含zookeeper全部数据的事务日志和快照日志文件，并转为tar.gz的压缩包保存到本地。  
你可以通过该压缩包将zookeeper的数据迁往其它的zookeeper。

> 建议在Leader上操作，保证数据没有丢失。  
> 非Leader上操作，建议在操作前执行sync同步一下。

# 简单使用
通过以下命令，可以在当前目录下生成压缩包。
```
java -DconfigPath=./zoo.cfg -jar TarZookeeper.jar
```

# 帮助
```
~ % java -jar TarZookeeper.jar help
Usage:
TarZookeeper -DconfigPath=[configPath] -DtxnDir=[txnDir] -DsnapDir=[snapDir] -DtarDir=[tarDir] -DsnapCount=[snapCount]
        configPath -- path to the zookeeper config file
        txnDir -- path to the txn directory
        snapDir -- path to the snap directory
        tarDir -- path to the compress directory, default is ./
        snapNum -- the number of snaps you want

```
