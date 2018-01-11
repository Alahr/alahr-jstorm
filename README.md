# 1 alahr-jstorm
software information
jstorm-2.2.1
kafka_2.9.2_0.8.2.2
mysql-5.5.17


# 2 project information

## 2.1 common module
common 工具类包

## 2.2 example module

### 2.2.1 word count example
单词统计：统计file/input.txt中的单词个数，并输出到file/output.txt中。
仅在本地模式运行；远程集群模式不识别file/input.txt和file/output.txt文件。

### 2.2.2 join on example
两表关联（类似select ... left on...）
实现person表和animal表关联，是左连接、右连接、内连接还是外连接，可以通过程序设计。
mysql配置信息在common/resource/database.properties中

### 2.2.3 back press example
反压（back pressure）
jstorm默认开启反压机制，在bolt处理不及时，让spout停止或减缓发送tuple
若关闭反压，bolt在短时间内充满ExecuteQueue

## 2.3 jstorm kafka module
jstorm消费kafka数据，结果显示在日志中。
目前存在的问题：消费完kafka数据后，程序会报错。



