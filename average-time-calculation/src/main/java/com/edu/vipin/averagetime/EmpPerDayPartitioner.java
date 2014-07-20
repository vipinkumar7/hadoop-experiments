package com.edu.vipin.averagetime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author Vipin Kumar
 * 
 */
public class EmpPerDayPartitioner extends Partitioner<EmpPerDay, LongWritable> {

	@Override
	public int getPartition(EmpPerDay key, LongWritable value, int numPartitions) {
		return key.getEmpId().hashCode() % numPartitions;
	}

}
