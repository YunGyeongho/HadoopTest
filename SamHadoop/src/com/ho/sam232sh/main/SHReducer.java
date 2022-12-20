package com.ho.sam232sh.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SHReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private static final IntWritable COUNT = new IntWritable();
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : arg1) {
			sum += i.get();
		}
		COUNT.set(sum);
		arg2.write(arg0, COUNT);
	}
}
