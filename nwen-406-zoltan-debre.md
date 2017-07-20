# Hadoop Assignment - NWEN 406

Zoltan Debre - 300360191

Original repository: https://github.com/zoltan-nz/hadoop

## PART 0 - Setup Hadoop on MacOS

My goal with this assignment is not only to be able to run Hadoop tasks on a previously created environment, but also I would like to learn and setup Hadoop environment from scratch. Additionally, I prefer fully reproducible, portable solution, which can be installed by anyone, without using a predefined setup, so it can work outside of our campus. For this reason I will use `maven` for managing Java dependencies.

Firstly, I setup a development environment on MacOS.

**Prerequisites, documentation**

* Brew package manager: [Homebrew](https://brew.sh/)
* [Setting up Hadoop on Mac OSX](http://zhongyaonan.com/hadoop-tutorial/setting-up-hadoop-2-6-on-mac-osx-yosemite.html)

**Install Hadoop**

```
$ brew install java
$ brew install maven
$ brew install hadoop
```

Run `brew info hadoop` to get the install directory, which need to setup `HADOOP_HOME` system environment variable.

Setup ENV variables in bash or zsh profile:

```
export JAVA_HOME="$(/usr/libexec/java_home)"
export HADOOP_HOME=path from 'brew info hadoop'
export HADOOP_PREFIX=$HADOOP_HOME/libexec
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
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

Browse the web interface for the NameNode:

```
$ open http://localhost:50070/
```
![Admin screen][screenshot-1]

Open The ResourceManager:

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

## PART 1 - Map Reduce Tutorial with Maven

I prefer to use a modern IntelliJ IDEA editor for Java projects with Maven package manager.

We can use Maven archetype to create a simple Java project quickly and Maven can download package dependencies also. In this case we need a few Hadoop packages.

A useful article in this topic:

* http://www.soulmachine.me/blog/2015/01/30/debug-hadoop-applications-with-intellij/

Create the skeleton project using Maven from command line.

```
$ mvn archetype:generate -DgroupId=nz.zoltan -DartifactId=wordcount -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
```

Other option, using IntelliJ IDEA create new project with `org.apache.maven.archetypes:maven-archetype-quickstart`

Our new project will be generated inside the `wordcount` folder of this repository. 

Maven creates a default `App.java` file and connected test file. We can delete these files. Create a new `WordCount.java`. The file structure should look like the following.

```
$ tree wordcount
wordcount
├── pom.xml
├── src
│   ├── main
│   │   └── java
│   │       └── nz
│   │           └── zoltan
│   │               ├── HelloWorld.java
│   │               └── WordCount.java
│   └── test
│       └── java
│           └── nz
│               └── zoltan
│                   └── HelloWorldTest.java
```

We have to update the `pom.xml` to setup the Hadoop dependencies:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>nz.zoltan</groupId>
  <artifactId>wordcount</artifactId>
  <packaging>jar</packaging>
  <version>1.0-SNAPSHOT</version>
  <name>wordcount</name>
  <url>http://maven.apache.org</url>

  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <hadoop.version>2.8.1</hadoop.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>${hadoop.version}</version>
    </dependency>

    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-core</artifactId>
      <version>LATEST</version>
    </dependency>
    
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>3.8.1</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <version>2.4</version>
        <configuration>
          <archive>
            <manifest>
              <mainClass>nz.zoltan.WordCount</mainClass>
            </manifest>
          </archive>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

`WordCount.java`:

```java
// Original source: https://ecs.victoria.ac.nz/foswiki/pub/Courses/NWEN406_2017T2/LabTutorial1/WordCount.java
package nz.zoltan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
```

Create a `wordcount/jobs` folder for Hadoop jobs. We can save in this folder all the text files what we would like to process with our WordCount app.

Using IntelliJ IDEA help us to debug our source code interactively. We can jump into the Hadoop source code also, because IntelliJ can download automatically the connected packages source files.

There are three options to launch our map reduce app. One of them as simple Java process, using the IntelliJ `Run` option. Second option is using `mvn exec:java`. Third option is using `hadoop jar`.

Change directory:

```
$ cd wordcount
```

Using `mvn exec`:

```
$ mvn exec:java -Dexec.args="jobs/example1/input jobs/example1/output"  
```

Building the jar file:

```
$ mvn clean package
```

Using `hadoop`:

```
$ hadoop jar target/wordcount-1.0-SNAPSHOT.jar jobs/example1/input jobs/example1/output
```

However, the last solution will not work, if we have more `main` class in the same `.jar` project built by maven.

**WordCount2.java**

Original source: https://ecs.victoria.ac.nz/foswiki/pub/Courses/NWEN406_2017T2/LabTutorial1/WordCount2.java

```java
// Source: https://ecs.victoria.ac.nz/foswiki/pub/Courses/NWEN406_2017T2/LabTutorial1/WordCount2.java
package nz.zoltan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        enum CountersEnum {INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
                Counter counter = context.getCounter(CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

One of the option to run different class from the same `.jar` file is the following:

```
mvn exec:java -Dexec.mainClass="nz.zoltan.WordCount2" -Dexec.args="jobs/example2/input jobs/example2/output"
mvn exec:java -Dexec.mainClass="nz.zoltan.WordCount2" -Dexec.args="jobs/example3/input jobs/example3/output"
```

Please note, there is a small bug in Hadoop 2.8 and earlier version which caused by `StatisticsDateReferenceCleaner`, because it swallows interrupt exceptions. You will see a `WARNING` note when we run the above `maven` commands, but we can ignore it now.

I realized, the best for running our task using the IntelliJ IDEA run option, because in this case we can setup different params.

Program arguments with case sensitive enabled and with skip patterns:

```
-Dwordcount.case.sensitive=true jobs/example3/input jobs/example3/case-true-with-skip -skip jobs/example3/patterns.txt
``` 

**Patent Citation Example**

You can find `PatentCitation.java` file in `./wordcount/src/main/java/nz/co.blogspot.anlisaldhana/` folder.

I moved the `cite75_99.txt` to `./wordcount/jobs/patent-citation/input` folder.

I run the the `PatentCitation` with IntelliJ runner with the following params:

Program arguments: `jobs/patent-citation/input jobs/patent-citation/output`
And the working directory is point to `wordcount` folder.

The result saved in `jobs/patent-citation/output` folder.