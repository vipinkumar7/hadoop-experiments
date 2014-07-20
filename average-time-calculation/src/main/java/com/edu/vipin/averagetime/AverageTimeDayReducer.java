package com.edu.vipin.averagetime;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Vipin Kumar
 * 
 */
public class AverageTimeDayReducer extends
		Reducer<EmpPerDay, LongWritable, Text, Text> {

	private Text timeavg = new Text();
	private Text empDate = new Text();
	private long inTime = 0;
	private long totalIntime = 0;
	private long outTime = 0;
	private long totalOutTime = 0;
	private boolean switchinOut = false;

	@Override
	protected void reduce(EmpPerDay empPerDay,
			Iterable<LongWritable> timestamps, Context context)
			throws IOException, InterruptedException {
		Set<Long> times = new TreeSet<Long>();
		Iterator<LongWritable> timestampsitr = timestamps.iterator();
		while (timestampsitr.hasNext()) {
			times.add(timestampsitr.next().get());
		}

		inTime = outTime = totalIntime = totalOutTime = 0;
		switchinOut = false;
		Iterator<Long> timestampsitrl = times.iterator();

		while (timestampsitrl.hasNext()) {
			if (switchinOut) {
				outTime = timestampsitrl.next();
				totalIntime += (inTime == 0) ? 0 : outTime - inTime;
				switchinOut = false;

			} else {
				inTime = timestampsitrl.next();
				totalOutTime += (outTime == 0) ? 0 : inTime - outTime;
				switchinOut = true;
			}
		}

		totalIntime = (long) (totalIntime / 3600000.00);
		totalOutTime = (long) (totalOutTime / 3600000.00);
		timeavg.set(totalIntime + "\t" + totalOutTime);
		empDate.set(empPerDay.getEmpId() + "\t" + empPerDay.getYear() + "-"
				+ empPerDay.getMonth() + "-" + empPerDay.getDay() + "\t");
		context.write(empDate, timeavg);
	}
}
