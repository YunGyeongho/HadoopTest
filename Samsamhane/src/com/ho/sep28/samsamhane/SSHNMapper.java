package com.ho.sep28.samsamhane;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SSHNMapper extends Mapper<Object, Text, Text, LongWritable>{
	private static final Text WORDS = new Text();
	private static final LongWritable ONE = new LongWritable(1);
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, " ");
		while(st.hasMoreTokens()) {
			WORDS.set(st.nextToken());
			context.write(WORDS, ONE);
		}
	}
}
