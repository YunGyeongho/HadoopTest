package com.ho.sep222wc.main;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable>{
	// 그리고 이제 java니까 작정하고 메모리 관리
	private static final Text WORD = new Text();
	private static final IntWritable ONE = new IntWritable(1);
	
	@Override
	protected void map(
			Object key, // 키? : 데이터가 있나 없나 체크 해보려면 필요
			Text value, // 그 문장 : 들어올 문장
			Mapper<Object, Text, Text, IntWritable>.Context context) // 결과 처리용
			throws IOException, InterruptedException {
		
		String line = value.toString();
		// String[] line2 = line.split(" "); 정형 데이터에 어울림
		StringTokenizer st = new StringTokenizer(line, " "); 
		//책은 비정형이니 이걸로 하는게 적합
		while(st.hasMoreTokens()) {
			WORD.set(st.nextToken());
			// 그냥 string과 int 1을 넣게 되면 hadoop이 쓸 수 없기에 hadoop에서 쓰는 Text와 IntWritable을 이용해준다.
			context.write(WORD, ONE);
		}
	}
}
