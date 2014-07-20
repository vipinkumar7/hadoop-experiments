package com.edu.vipin.averagetime;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author Vipin Kumar
 * 
 *         The group comparator is written so that for an employee each date is
 *         considered as a separate entity
 * 
 */
public class AverageTimeDayGroupComparator extends WritableComparator {

	public AverageTimeDayGroupComparator() {
		super(EmpPerDay.class, true);
	}

	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable a,
			@SuppressWarnings("rawtypes") WritableComparable b) {
		EmpPerDay empPerDay = (EmpPerDay) a;
		EmpPerDay empPerDay1 = (EmpPerDay) b;
		int comp = empPerDay.getEmpId().compareTo(empPerDay1.getEmpId());
		if (comp == 0)
			comp = empPerDay.getYear().compareTo(empPerDay1.getYear());
		if (comp == 0)
			comp = empPerDay.getMonth().compareTo(empPerDay1.getMonth());
		if (comp == 0)
			comp = empPerDay.getDay().compareTo(empPerDay1.getDay());

		return comp;

	}
}
