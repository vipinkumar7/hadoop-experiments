package com.edu.vipin.averagetime;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Vipin Kumar
 * 
 *         Read Time-Stamp logs and make an employee entity with empid,year
 *         ,month and date and put the time stamp against it.
 * 
 * 
 */
public class AverageTimeDayMapper extends
		Mapper<LongWritable, Text, EmpPerDay, LongWritable> {

	private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger
			.getLogger(AverageTimeDayMapper.class);

	private EmpPerDay empPerDay = new EmpPerDay();
	private LongWritable timestamp = new LongWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] empTimeStamp = line.split("\t");
		long empId = Long.parseLong(empTimeStamp[0]);
		String fulldate = empTimeStamp[1];

		SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS");
		Date date = null;
		try {
			date = format.parse(fulldate);
			SimpleDateFormat yf = new SimpleDateFormat("yyyy");
			String year = yf.format(date);
			SimpleDateFormat mf = new SimpleDateFormat("MM");
			String month = mf.format(date);
			SimpleDateFormat df = new SimpleDateFormat("dd");
			String day = df.format(date);

			empPerDay.set(empId, Integer.parseInt(year),
					Integer.parseInt(month), Integer.parseInt(day));

			timestamp.set(date.getTime());

		} catch (ParseException e) {
			LOG.debug("eroor in parsing the date");
		}
		context.write(empPerDay, timestamp);

	}
}
