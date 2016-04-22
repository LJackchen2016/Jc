package com.jc.mathod;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * 将不同时间里消费的同学和消费记录组成一个表
 * @author JackChen
 *
 */
public class AccountAndTrade {
	
	
	/**
	 * 实现trade文件的操作
	 * @author JackChen
	 *
	 */
	static class AATMapper1 extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String v;
			String k;
			String[] line = value.toString().split("\t");	
			if(line[0].length()<=2){
				v = line[4]+"	"+line[3];
				k = line[1];
			}else{
				v = line[5]+line[2];
				k = line[0];
			}
			context.write(new Text(k.trim()), new Text(v.trim()));
		}
	}
	
	
	 static class AATReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//取出学生的信息。
			 while(values.iterator().hasNext()){
				 Text t = values.iterator().next();
				 context.write(key, t); 
			 }
		 }
	}
	
	
	public void run(Integer i) throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     for(Integer k=1; k<=7; k++){
	    	 String dst1 = "hdfs://192.168.192.128:8020/output/gettxtbyhour/"+k.toString()+"/"+i.toString()+"-m-00000"; 
	    	 MultipleInputs.addInputPath(job, new Path(dst1),TextInputFormat.class, AATMapper1.class);
	     }
	     String dst2 = "hdfs://192.168.192.128:8020/input/account.txt"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/countandtrade/"+i.toString()+"/";
		 
		 
//		 FileInputFormat.addInputPath(job, new Path(dst1));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(AccountAndTrade.class);
	   //  job.setMapperClass(AATMapper1.class);
	     
	     MultipleInputs.addInputPath(job, new Path(dst2),TextInputFormat.class, AATMapper1.class);
		 job.setCombinerClass(AATReducer.class);
		 job.setReducerClass(AATReducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 
		 try {
			 job.waitForCompletion(true);
		 } catch (ClassNotFoundException e) {
			 e.printStackTrace();
		 } catch (InterruptedException e) {
			 e.printStackTrace();
		 }
	 }
	
	
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		
		AccountAndTrade aat = new AccountAndTrade();
		for(int i=1; i<23; i++){
			aat.run(i);
		}
		
	}

}
