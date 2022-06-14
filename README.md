# 介绍
TarZookeeper读取zoo.cfg后，选取能够包含zookeeper全部数据的事务日志和快照日志，并转为tar.gz的压缩包。

> 建议在Leader上操作，保证数据没有丢失。