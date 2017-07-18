# Hadoop Assignment - NWEN 406

Zoltan Debre - 300360191

Original repository: https://github.com/zoltan-nz/hadoop

## PART 0 - Setup Hadoop on MacOS

My goal with this assignment is not only to be able to run Hadoop tasks on a previously created environment, but also I would like learn and setup Hadoop environment from scratch.

Firstly, I setup a development environment on MacOS.

**Prerequisites, documentation**

* Brew package manager: [Homebrew](https://brew.sh/)
* [Setting up hadoop on Mac OSX](http://zhongyaonan.com/hadoop-tutorial/setting-up-hadoop-2-6-on-mac-osx-yosemite.html)

**Install Hadoop**

```
$ brew install java
$ brew install hadoop
```

Run `brew info hadoop` to get the install directory, which need to setup `HADOOP_HOME` system environment variable.

Setup ENV variables in bash or zsh profile:

```
export JAVA_HOME="$(/usr/libexec/java_home)"
export HADOOP_HOME=path from 'brew info hadoop'
export HADOOP_PREFIX=$HADOOP_HOME/libexec
```

**Start Hadoop**

```
$ cd $HADOOP_HOME
$ bin/hdfs namenode -format
$ sbin/start-dfs.sh
$ sbin/start-yarn.sh
```

**Create user folder**

```
$ bin/hdfs dfs -mkdir /user
$ bin/hdfs dfs -mkdir /user/zoltan
```

**Open Hadoop admin pages**

```
$ open http://localhost:50070/
```
![Admin screen][screenshot-1]

```
$ open http://localhost:8088/
```
![Cluster info screen][screenshot-2]

```
$ open http://localhost:8042
```
![Node manager screen][screenshot-3]

[screenshot-1]:images/admin-screen.png
[screenshot-2]:images/all-application-screen.png
[screenshot-3]:images/node-manager-screen.png 

## PART 1


