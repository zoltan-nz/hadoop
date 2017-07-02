THE HADOOP EXPERIENCE
=====================

Install Hadoop on Mac:

    $ brew install hadoop
    
Optional:
    
    $ brew install java
    $ brew install gradle

Optional environment settings

    export JAVA_HOME="$(/usr/libexec/java_home)"
    export HADOOP_PREFIX=$HADOOP_HOME/libexec


Settings on MacOSX:

* http://zhongyaonan.com/hadoop-tutorial/setting-up-hadoop-2-6-on-mac-osx-yosemite.html
* Check you have access to your localhost with `ssh localhost`. Otherwise setup ssh keys.

Tutorials:

* [MapReduce Tutorial r2.8.0](http://hadoop.apache.org/docs/r2.8.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
* [Victuria University Tutorial](https://ecs.victoria.ac.nz/Courses/NWEN406_2017T2/LabTutorial1)

Docker image:

* [hadoop-docker](https://github.com/sequenceiq/hadoop-docker)

```
$ hdfs dfs -ls
$ hdfs dfs -ls hdfs://localhost:9000/user
```

```
docker build -t sequenceiq/hadoop-docker .
```