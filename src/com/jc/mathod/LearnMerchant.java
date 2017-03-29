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


public class LearnMerchant {

	
	static class LMMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String v;
			String k;
			String[] line = value.toString().split("\t");	
			context.write(new Text(line[0].trim()), new Text(line[1].trim()+"	"+line[2].trim()+"	"+line[3].trim()));
		}
	}
	
	
	 static class LMReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//取出学生的信息。
			 while(values.iterator().hasNext()){
				 Text t = values.iterator().next();
				 context.write(key, t);
			 }
			 context.write(new Text(), new Text("-------------------"));
		 }
	}
	
	
	public void run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst1 = "hdfs://192.168.192.128:8020/input/merchant.txt"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/learnmerchant/";
		 
		 
		 FileInputFormat.addInputPath(job, new Path(dst1));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(LearnMerchant.class);
	     MultipleInputs.addInputPath(job, new Path(dst1),TextInputFormat.class, LMMapper.class);
		 job.setReducerClass(LMReducer.class);
		 
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
		
		LearnMerchant lm = new LearnMerchant();
		lm.run();

	}
	
}
