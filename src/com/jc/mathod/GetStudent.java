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
 * 用于取一定数量的各种类型学生，便于后面学生的分类。
 * @author JackChen
 *
 */
public class GetStudent {

	
	static Integer bknan = 0;		static Integer bknv = 0;//本科男和本科女
	static Integer ssnan = 0;		static Integer ssnv = 0;//硕士男和硕士女
	static Integer bsnan = 0;		static Integer bsnv = 0;//博士男和博士女
	
	static class GSMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");	
			String stuType = line[5]+line[2];
			if(stuType.equals("本科男")){
				if(bknan < 200){
					bknan++;
					context.write(new Text(""), new Text(value));
				}
			}else if(stuType.equals("本科女")){
				if(bknv < 100){
					bknv++;
					context.write(new Text(""), new Text(value));
				}
			}else if(stuType.equals("硕士男")){
				if(ssnan < 100){
					ssnan++;
					context.write(new Text(""), new Text(value));
				}	
			}else if(stuType.equals("硕士女")){
				if(ssnv < 50){
					ssnv++;
					context.write(new Text(""), new Text(value));
				}	
			}else if(stuType.equals("博士男")){
				if(bsnan < 50){
					bsnan++;
					context.write(new Text(""), new Text(value));
				}	
			}else if(stuType.equals("博士女")){
				if(bsnv < 25){
					bsnv++;
					context.write(new Text(""), new Text(value));
				}
			}
		}
	}
	
	
	 static class GSReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			 while(values.iterator().hasNext()){
				 Text text = values.iterator().next();
				 context.write(new Text(""), new Text(text));
				 
			 }
				 
		 }
	}
	
	
	public void run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst = "hdfs://192.168.192.128:8020/input/account.txt/"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/getstudent1/";
		 
		 
		 FileInputFormat.addInputPath(job, new Path(dst));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(GetStudent.class);
	     MultipleInputs.addInputPath(job, new Path(dst),TextInputFormat.class, GSMapper.class);
		 job.setReducerClass(GSReducer.class);
		 
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
		
		GetStudent gs = new GetStudent();
		gs.run();
		
	}
	
	
	
}
