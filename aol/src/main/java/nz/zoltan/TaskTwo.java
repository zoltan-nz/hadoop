package nz.zoltan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TaskTwo extends Configured implements Tool {
	private static final String NUMBER_OF_SEARCH_JOB_NAME = "task2-searches";
	private static final String NUMBER_OF_USERS_JOB_NAME = "task2-users";
	private static final String NUMBER_OF_CLICKS_JOB_NAME = "task2-clicks";

	// Using for counting simple rows
	private static final LongWritable ONE = new LongWritable(1);

	/**
	 * Generates a simple KEY -> VALUE list, where key is a unique ID generated by Hadoop and the value is "1",
	 * so finally the reducer is a simple counter.
	 */
	public static class NumberOfSearchMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		protected void map(LongWritable key, Text searchLogLine, Context context) throws IOException, InterruptedException {

			String[] columns = searchLogLine.toString().split("\t");

			try {
				Integer.parseInt(columns[0]);
			} catch (NumberFormatException e) {
				return; // The data in the first column is not an integer, read the next line.
			}

			// This is a basic line counter implementation, if a search logged, we send a counter with the key
			// to the reducer, so the reducer can simply just sum how many rows we have.
			context.write(new Text(key.toString()), ONE);
		}
	}

	public static class NumberOfSearchReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		Long numberOfSearchCounter = 0L;

		protected void reduce(Text key, Iterable<LongWritable> ones, Context context) throws IOException, InterruptedException {

			for (LongWritable ignored : ones) {
				numberOfSearchCounter++;
			}

			String message = "Number of Searches: ";

			context.write(new Text(message), new LongWritable(numberOfSearchCounter));
		}

	}

	/**
	 * For counting the number of unique users we need a Mapper, a Combiner and a Reducer.
	 * Mapper creates a KEY -> VALUE list, where KEY is the userId (ANON_ID), and the VALUE is a simple "1"
	 * <p>
	 * The Combiner can work as an aggregator, because the reducer function will be called with each individual userId.
	 * Basically, we have to count, how many times the combiner was called.
	 * <p>
	 * From the combiner, we just generate a list of "users" -> 1 list, so a simple summary of lines gives us the
	 * requested value. This step can be done in the last reducer call.
	 */
	public static class NumberOfUsersMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		protected void map(LongWritable key, Text searchLogLine, Context context) throws IOException, InterruptedException {

			String[] columns = searchLogLine.toString().split("\t");

			Integer userId;

			try {
				userId = Integer.parseInt(columns[0]);
			} catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
				return; // The data in the first column is not an integer or there isn't any data read the next line.
			}

			context.write(new Text(userId.toString()), ONE);
		}
	}

	public static class NumberOfUsersCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

		protected void reduce(Text userId, Iterable<LongWritable> ones, Context context) throws IOException, InterruptedException {
			String key = "users";
			context.write(new Text(key), ONE);
		}
	}

	public static class NumberOfUsersReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		protected void reduce(Text userId, Iterable<LongWritable> ones, Context context) throws IOException, InterruptedException {

			Long numberOfUsersCounter = 0L;

			for (LongWritable ignored : ones) {
				numberOfUsersCounter++;
			}

			String message = "Number of Users: ";

			context.write(new Text(message), new LongWritable(numberOfUsersCounter));
		}
	}

	/**
	 * Counting clicks logic is similar to counting searches, the only differences is the filter logic in Mapper.
	 * <p>
	 * If a line does not have itemRank data, it means, it is not a click, so we ignore that line.
	 */
	public static class NumberOfClicksMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		protected void map(LongWritable key, Text searchLogLine, Context context) throws IOException, InterruptedException {

			String[] columns = searchLogLine.toString().split("\t");

			try {
				Integer.parseInt(columns[0]);
			} catch (NumberFormatException e) {
				return; // The data in the first column is not an integer, read the next line.
			}

			String itemRank = "";

			try {
				itemRank = columns[3];
			} catch (ArrayIndexOutOfBoundsException e) {
				return; // No itemRank, no click, try the next.
			}

			// If the search log line contains an itemRank than it was clicked, so we can count one.
			context.write(new Text(key.toString()), ONE);
		}
	}

	public static class NumberOfClicksReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		Long numberOfClicksCounter = 0L;

		protected void reduce(Text key, Iterable<LongWritable> ones, Context context) throws IOException, InterruptedException {

			for (LongWritable ignored : ones) {
				numberOfClicksCounter++;
			}

			String message = "Number of Clicks: ";

			context.write(new Text(message), new LongWritable(numberOfClicksCounter));
		}
	}

	public int run(String[] args) throws Exception {

		validateParams(args);

		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		deleteFilesInDirectory(outputDir);

		Path searchOutputDir = new Path(outputDir + "/search");
		Path usersOutputDir = new Path(outputDir + "/users");
		Path clicksOutputDir = new Path(outputDir + "/clicks");

		Job numberOfSearchJob = Job.getInstance(getConf(), NUMBER_OF_SEARCH_JOB_NAME);
		Job numberOfUsersJob = Job.getInstance(getConf(), NUMBER_OF_USERS_JOB_NAME);
		Job numberOfClicksJob = Job.getInstance(getConf(), NUMBER_OF_CLICKS_JOB_NAME);

		numberOfSearchJob.setJarByClass(TaskTwo.class);
		numberOfUsersJob.setJarByClass(TaskTwo.class);
		numberOfClicksJob.setJarByClass(TaskTwo.class);

		FileInputFormat.setInputPaths(numberOfSearchJob, inputDir);
		FileInputFormat.setInputPaths(numberOfUsersJob, inputDir);
		FileInputFormat.setInputPaths(numberOfClicksJob, inputDir);

		FileOutputFormat.setOutputPath(numberOfSearchJob, searchOutputDir);
		FileOutputFormat.setOutputPath(numberOfUsersJob, usersOutputDir);
		FileOutputFormat.setOutputPath(numberOfClicksJob, clicksOutputDir);

		numberOfSearchJob.setInputFormatClass(TextInputFormat.class);
		numberOfUsersJob.setInputFormatClass(TextInputFormat.class);
		numberOfClicksJob.setInputFormatClass(TextInputFormat.class);

		numberOfSearchJob.setOutputFormatClass(TextOutputFormat.class);
		numberOfUsersJob.setOutputFormatClass(TextOutputFormat.class);
		numberOfClicksJob.setOutputFormatClass(TextOutputFormat.class);

		numberOfSearchJob.setOutputKeyClass(Text.class);
		numberOfUsersJob.setOutputKeyClass(Text.class);
		numberOfClicksJob.setOutputKeyClass(Text.class);

		numberOfSearchJob.setOutputValueClass(LongWritable.class);
		numberOfUsersJob.setOutputValueClass(LongWritable.class);
		numberOfClicksJob.setOutputValueClass(LongWritable.class);

		numberOfSearchJob.setMapperClass(NumberOfSearchMapper.class);
		numberOfSearchJob.setCombinerClass(NumberOfSearchReducer.class);
		numberOfSearchJob.setReducerClass(NumberOfSearchReducer.class);

		numberOfUsersJob.setMapperClass(NumberOfUsersMapper.class);
		numberOfUsersJob.setCombinerClass(NumberOfUsersCombiner.class);
		numberOfUsersJob.setReducerClass(NumberOfUsersReducer.class);

		numberOfClicksJob.setMapperClass(NumberOfClicksMapper.class);
		numberOfClicksJob.setCombinerClass(NumberOfClicksReducer.class);
		numberOfClicksJob.setReducerClass(NumberOfClicksReducer.class);

		return (numberOfSearchJob.waitForCompletion(false)
				&& numberOfUsersJob.waitForCompletion(false)
				&& numberOfClicksJob.waitForCompletion(false))
				? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new TaskTwo(), args));
	}

	private static void validateParams(String[] args) {

		int numberOfParams = args.length;

		if (numberOfParams != 2) {
			throw new IllegalArgumentException("TaskTwo expects exactly 2 params: inputFile outputFile");
		}
	}

	private static void deleteFilesInDirectory(Path f) throws IOException {

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		if (!hdfs.delete(f, true))
			throw new FileNotFoundException("Failed to delete file: " + f);
	}
}
