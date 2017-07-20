/*
 * From http://everythingbigdata.blogspot.co.nz/2012/05/apache-hadoop-map-reduce-advanced.html
 * Derived from
 * Listing 4.1 from
 * Hadoop in Action
 * Chuck Lam
 * 2011, Manning Publications Co.
 */

package nz.co.blogspot.anilsaldhana;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.FileSystem;

public class PatentCitation extends Configured implements Tool {

    public static class PatentCitationMapper extends Mapper<Text, Text, Text, Text> {
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] citation = key.toString().split(",");
            context.write(new Text(citation[1]), new Text(citation[0]));
        }
    }

    public static class PatentCitationReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String csv = "";
            for (Text val : values) {
                if (csv.length() > 0) csv += ",";
                csv += val.toString();
            }
            context.write(key, new Text(csv));
        }
    }

    private void deleteFilesInDirectory(Path f) throws IOException {
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);
        if (!hdfs.delete(f, true))
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int run(String[] args) throws Exception {
        if (args.length == 0)
            throw new IllegalArgumentException("Please provide input and output paths");

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        deleteFilesInDirectory(outputDir);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "Hadoop Patent Citation Example");
        job.setJarByClass(PatentCitation.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(PatentCitationMapper.class);
        job.setReducerClass(PatentCitationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //job.setNumReduceTasks(10000);

        return job.waitForCompletion(false) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PatentCitation(), args));
    }
}