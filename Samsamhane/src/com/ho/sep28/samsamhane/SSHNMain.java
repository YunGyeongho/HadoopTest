package com.ho.sep28.samsamhane;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SSHNMain {
	public static void main(String[] args) {
		try {
			Job j = Job.getInstance(new Configuration());
			
			j.setMapperClass(SSHNMapper.class);
			j.setCombinerClass(SSHNReducer.class);
			j.setReducerClass(SSHNReducer.class);
			
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(LongWritable.class);
			
			String f = null;
			int i = 0;
			for (i = 1; i < 11; i++) {
				f = String.format("/sam%02d.txt", i);
				FileInputFormat.addInputPath(j, new Path(f));
			}
			// 삼국지 1~10권 까지 분석 돌려줄 반복문
			// 어차피 이 .jar은 삼국지 분석 밖에 못해서 명령어로 유동성 있게 파일을 지정할 필요가 없으니 지정해줬다.
			
			FileOutputFormat.setOutputPath(j, new Path(args[0]));
			
			j.waitForCompletion(true);
			// 분석 끝날 때 까지 기달려줘.
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
