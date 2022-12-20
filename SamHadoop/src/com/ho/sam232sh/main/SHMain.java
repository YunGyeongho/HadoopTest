package com.ho.sam232sh.main;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SHMain {
	public static void main(String[] args) {
		try {
			Job j = Job.getInstance(new Configuration());
			
			j.setMapperClass(SHMapper.class);
			j.setCombinerClass(SHReducer.class);
			// combiner는 자동 처리라 딱히 클래스를 넣을 필요는 없지만 적긴 해야 되서 적는다.
			// 안 되는지 테스트 하고 싶다만 실패하면 타격이 너무 커서 그냥 하던대로 해준다.
			j.setReducerClass(SHReducer.class);
			
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			
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
