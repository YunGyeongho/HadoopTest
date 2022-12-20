package com.ho.sep28.samsamhane;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SSHNReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	private static final LongWritable COUNT = new LongWritable();
	
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable i : arg1) {
			sum += i.get();
		}
		COUNT.set(sum);
		arg2.write(arg0, COUNT);
	}
}
