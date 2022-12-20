package com.ho.sep222wc.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private static final IntWritable SUM = new IntWritable();
	
	
	// 결과 한 덩어리 마다 자동호출
	@Override
	protected void reduce(
		Text arg0, //앞에서 잘려서 들어온 단어
		Iterable<IntWritable> arg1, // 합쳐져서 1, 1 이런식으로 되어있을 숫
		Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) // 결과 처리
				throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : arg1) {
			sum += i.get();
		}
		SUM.set(sum);
		arg2.write(arg0, SUM);
	}
}
