package com.ho.sam232sh.main;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SHMapper extends Mapper<Object, Text, Text, IntWritable>{
	private static final Text NAME = new Text();
	private static final IntWritable ONE = new IntWritable(1);
	
	// Hadoop 서버 각각의 컴퓨터에서 문장 하나 만날 때 마다 불러올 메소드
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// Java에서 쓰던 String이 아닌 Hadoop의 Text이기에 우리가 다듬을 수 있게 자료형을 바꿔준다.
		String line = value.toString();
		// 들어온 문장을 spacebar로 구분해 뜯어서 반복문
		StringTokenizer st = new StringTokenizer(line, " ");
		String word = null;
		while(st.hasMoreTokens()) {
			word = st.nextToken();
			// 데이터를 분석하려면 우선 그 데이터에 대한 배경지식이 필요하다.
			// 유비의 다른 명칭 또한 전부 알고 있어야 유비를 걸러낼 수 있다. 근데 난 잘 모른다..
			if(word.contains("유비") || word.contains("현덕")) {
				// 유비랑, 유비님, 현덕씨 등등 전부 걸릴텐데 그 모든걸 다 유비 로 셋팅 해준다.
				NAME.set("유비");
				// 셋팅한 NAME과 카운트용 ONE을 결과 처리. 밑에 조조, 손권도 동일.
				context.write(NAME, ONE);
				// 만약 들어온 문장이 유비가 유비 집에 갔더니 유비 와이프가 유비 밥을 먹고 있었다.
				// 이런식으로 생겼다면 유비가 1번 언급 된거랑 마찬 가지이니 break를 걸어줌.
				break;
			} else if (word.contains("조조") || word.contains("맹덕")) {
				NAME.set("조조");
				context.write(NAME, ONE);
				break;
			} else if (word.contains("손권") || word.contains("중모")) {
				NAME.set("손권");
				context.write(NAME, ONE);
				break;
			}
		}
		
	}
}
