package com.ho.sep222wc.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCMain {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			// Job 패키지명 짧은거 
			Job j = Job.getInstance(conf);
			
			// 각 단계별 이용할 클래스 셋팅
			j.setMapperClass(WCMapper.class);
			j.setCombinerClass(WCReducer.class);
			j.setReducerClass(WCReducer.class);
			
			// 다 한거 뭘로 받을지
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			
			// Format 패키지명 긴거 I/O 둘다
			FileInputFormat.addInputPath(j, new Path(args[0]));
			// new Path에 파일 경로 자체를 써버리면 무조건 거기에 결과를 가져다 놓을 테니
			// 다른 책에도 적용하려면 명령어(args)의 첫번째 index 경로에 갖다놓아라, 밑에는 두번째 경로에 
			FileOutputFormat.setOutputPath(j, new Path(args[1]));
			
			//기본적으로 백그라운드에서 동작 해서 안 보이기 때문에 포어그라운드 동작하면서 메세지 보여달라고 하는 것.
			j.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
			
	}
}
