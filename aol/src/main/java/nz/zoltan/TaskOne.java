package nz.zoltan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TaskOne extends Configured implements Tool {
	private static final String JOB_NAME = "task1";
	private static final String ANON_ID = "anonId";

	public static class TaskOneMapper extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text searchLogLine, Context context) throws IOException, InterruptedException {
			int anonIdFilter = context.getConfiguration().getInt(ANON_ID, 0);
			int anonId = 0;

			String[] columns = searchLogLine.toString().split("\t");

			try {
				anonId = Integer.parseInt(columns[0]);
			} catch (NumberFormatException e) {
				return; // The data in the first column is not an integer, read the next line.
			}

			if (anonIdFilter != anonId) return;

			String id = "";
			String query = "";
			String itemRank = "";
			String clickUrl = "";

			try {
				id = columns[0];
				query = columns[1];
				itemRank = columns[3];
				clickUrl = columns[4];
			} catch (ArrayIndexOutOfBoundsException e) {
			}

			String result = id + ", " + query + ", " + itemRank + ", " + clickUrl;

			context.write(new Text(id), new Text(result));
		}
	}

	public static class TaskOneReducer extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text anonId, Iterable<Text> searchResults, Context context) throws IOException, InterruptedException {

			StringBuilder csvExport = new StringBuilder();

			for (Text result : searchResults)
				csvExport
						.append(result.toString())
						.append("\n");

			String header = "ANON_ID, QUERY, ITEM_RANK, CLICK_URL\n";

			context.write(new Text(header), new Text(csvExport.toString()));
		}

	}

	public int run(String[] args) throws Exception {

		validateParams(args);

		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		int anonId = parseAnonId(args[2]);

		deleteFilesInDirectory(outputDir);

		Job job = Job.getInstance(getConf(), JOB_NAME);
		job.setJarByClass(TaskOne.class);

		FileInputFormat.setInputPaths(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TaskOneMapper.class);
		job.setCombinerClass(TaskOneReducer.class);
		job.setReducerClass(TaskOneReducer.class);

		Configuration conf = job.getConfiguration();
		conf.setInt(ANON_ID, anonId);

		return job.waitForCompletion(false) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new TaskOne(), args));
	}

	private static void validateParams(String[] args) {

		int numberOfParams = args.length;

		if (numberOfParams != 3) {
			throw new IllegalArgumentException("TaskOne expects exactly 3 params: inputFile outputFile ANON_ID");
		}
	}

	private static int parseAnonId(String anonIdParam) {

		int anonId;

		try {

			anonId = Integer.parseInt(anonIdParam);
			if (anonId <= 0) throw new NumberFormatException();

		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid ANON_ID. It must be a positive integer.");
		}

		return anonId;
	}

	// Source from PatentCitation.java
	// From http://everythingbigdata.blogspot.co.nz/2012/05/apache-hadoop-map-reduce-advanced.html
	private static void deleteFilesInDirectory(Path f) throws IOException {

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		if (!hdfs.delete(f, true))
			throw new FileNotFoundException("Failed to delete file: " + f);
	}
}
