package com.ho.sep26.bh;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BHMain {
	public static void main(String[] args) {
		try {
			Job j = Job.getInstance(new Configuration());
			
			j.setMapperClass(BHMapper.class);
			j.setCombinerClass(BHReducer.class);
			j.setReducerClass(BHReducer.class);
			
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(LongWritable.class);
			
			FileInputFormat.addInputPath(j, new Path("/bus2015.csv"));
			FileInputFormat.addInputPath(j, new Path("/bus2016.csv"));
			FileInputFormat.addInputPath(j, new Path("/bus2017.csv"));
			FileInputFormat.addInputPath(j, new Path("/bus2018.csv"));
			FileInputFormat.addInputPath(j, new Path("/bus2019.csv"));
			FileInputFormat.addInputPath(j, new Path("/bus2020.csv"));
			FileOutputFormat.setOutputPath(j, new Path(args[0]));
			
			j.waitForCompletion(true);
		} catch (Exception e) {
		}
	}
}