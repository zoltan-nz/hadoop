THE HADOOP EXPERIENCE
=====================

Install Hadoop on Mac:

    $ brew install hadoop
    
Optional:
    
    $ brew install java
    $ brew install gradle

Environment settings

    export JAVA_HOME="$(/usr/libexec/java_home)"
    export HADOOP_HOME=...
    export HADOOP_PREFIX=$HADOOP_HOME/libexec
    export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
    export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
    export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar


Settings on MacOSX:

* http://zhongyaonan.com/hadoop-tutorial/setting-up-hadoop-2-6-on-mac-osx-yosemite.html
* https://dtflaneur.wordpress.com/2015/10/02/installing-hadoop-on-mac-osx-el-capitan/
* Check you have access to your localhost with `ssh localhost`. Otherwise setup ssh keys.

Tutorials:

* [MapReduce Tutorial r2.8.0](http://hadoop.apache.org/docs/r2.8.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
* [Victuria University Tutorial](https://ecs.victoria.ac.nz/Courses/NWEN406_2017T2/LabTutorial1)

Docker image:

* [hadoop-docker](https://github.com/sequenceiq/hadoop-docker)

Run Hadoop:

```
$ cd $HADOOP_HOME
$ bin/hdfs namenode -format
$ sbin/start-dfs.sh
$ open http://localhost:50070/
$ bin/hdfs dfs -mkdir /user
$ bin/hdfs dfs -mkdir /user/zoltan
$ sbin/start-yarn.sh
$ open http://localhost:8088/
$ open http://localhost:8042
```

Deprecated command:

```
$ ${HADOOP_HOME}/sbin/start-all.sh
```

List hdfs directory:

```
$ hdfs dfs -ls
$ hdfs dfs -ls hdfs://localhost:9000/user
```

Hadoop Docker container:

```
docker build -t sequenceiq/hadoop-docker .
```

Copy sample files in hdfs:

```
$ ${HADOOP_HOME}/bin/hdfs dfs -put ${HADOOP_HOME}/libexec/etc/hadoop /user/zoltan/input
```

## Class Tutorial

```
$ echo "Hello World Bye World" > file01
$ echo "Hello Hadoop Goodbye Hadoop" > file02

$ hdfs dfs -mkdir /user/zoltan/input
```

