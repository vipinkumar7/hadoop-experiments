package com.edu.vipin.averagetime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author Vipin Kumar
 * 
 *         Employee entity which acts as key to reducer
 * 
 */
public class EmpPerDay implements Writable, WritableComparable<EmpPerDay> {

	private LongWritable empId = new LongWritable();
	private IntWritable year = new IntWritable();
	private IntWritable month = new IntWritable();
	private IntWritable day = new IntWritable();

	public EmpPerDay() {

	}

	public void set(long empID, int year, int month, int day) {
		this.empId.set(empID);
		this.year.set(year);
		this.month.set(month);
		this.day.set(day);

	}

	public int compareTo(EmpPerDay empPerDay) {
		int compareValue = this.empId.compareTo(empPerDay.getEmpId());
		if (compareValue == 0)
			compareValue = this.year.compareTo(empPerDay.getMonth());
		if (compareValue == 0)
			compareValue = this.month.compareTo(empPerDay.getDay());
		if (compareValue == 0)
			compareValue = this.day.compareTo(empPerDay.getDay());
		return compareValue;
	}

	public static EmpPerDay read(DataInput in) throws IOException {
		EmpPerDay empPerDay = new EmpPerDay();
		empPerDay.readFields(in);
		return empPerDay;
	}

	public void write(DataOutput out) throws IOException {
		empId.write(out);
		year.write(out);
		month.write(out);
		day.write(out);

	}

	public void readFields(DataInput in) throws IOException {
		empId.readFields(in);
		year.readFields(in);
		month.readFields(in);
		day.readFields(in);

	}

	public LongWritable getEmpId() {
		return empId;
	}

	public IntWritable getYear() {
		return year;
	}

	public IntWritable getMonth() {
		return month;
	}

	public IntWritable getDay() {
		return day;
	}

}
