package com.edu.vipin.averagetime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Vipin Kumar
 * 
 */
public class AverageTimeCalDriver extends Configured implements Tool {

	private static final Log LOG = LogFactory
			.getLog(AverageTimeCalDriver.class);

	public static void main(String[] args) {

		try {
			ToolRunner.run(new Configuration(), new AverageTimeCalDriver(),
					args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		LOG.info("starting AverageTimeCalDriver");
		Configuration configuration = getConf();

		Job job = Job.getInstance(configuration, "averageTimecalculator");
		job.setJarByClass(AverageTimeCalDriver.class);

		FileInputFormat.addInputPath(job, new Path(
				"/home/hduser/Documents/Records.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/hduser/Documents/out/"));
		job.setPartitionerClass(EmpPerDayPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);

		/* set the no of reducer to get each employee report in one file */
		// job.setNumReduceTasks(2000);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setGroupingComparatorClass(AverageTimeDayGroupComparator.class);
		job.setSortComparatorClass(AverageTimeDayGroupComparator.class);
		job.setMapOutputKeyClass(EmpPerDay.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(AverageTimeDayMapper.class);
		job.setReducerClass(AverageTimeDayReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
}
