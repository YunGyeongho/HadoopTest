package com.ho.sep26.bh;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BHMapper extends Mapper<Object, Text, Text, LongWritable>{
	private static final Text YOIL = new Text();
	private static final LongWritable SUM = new LongWritable();
	private static final LongWritable ONE = new LongWritable(1);
	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMdd");
	private static final SimpleDateFormat E = new SimpleDateFormat("E");
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String[] line2 = line.split(",");
			StringBuffer sb = new StringBuffer();
			sb.append(line2[0]);
			sb.append(line2[1]);
			sb.append(line2[2]);
			
			YOIL.set(E.format(SDF.parse(sb.toString())) + "_합");
			SUM.set(Integer.parseInt(line2[5]) + Integer.parseInt(line2[6])); 
			context.write(YOIL, SUM);
			YOIL.set(E.format(SDF.parse(sb.toString())) + "_수");
			context.write(YOIL, ONE);
		} catch (Exception e) {
		}
	}
}
