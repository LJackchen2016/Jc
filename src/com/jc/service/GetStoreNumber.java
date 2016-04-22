package com.jc.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GetStoreNumber {

	static HashMap<String,Integer> hm = new HashMap<String,Integer>();//用于记录商铺号和对应得编号。
	static Integer num = 0;//记录商铺对应得编号。
	
	static class GSMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");	
			String storeType = line[2];
			context.write(new Text(storeType), new Text(""));
		}
	}
	
	
	 static class GSReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String str = "";
			 while(values.iterator().hasNext()){
				 Text text = values.iterator().next();
			 }
			 context.write(key, new Text(num.toString()));
			 hm.put(key.toString(), num);
			 num++;
		 }
	}
	
	
	public HashMap<String,Integer> run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst = "hdfs://192.168.192.128:8020/input/merchant.txt/"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/hh/";
		 
		 
		 FileInputFormat.addInputPath(job, new Path(dst));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(GetStoreNumber.class);
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
		 
		 return hm;
	 }
	
	public static void main(String[] args) throws IllegalArgumentException, IOException{
		
		GetStoreNumber gsn = new GetStoreNumber();
		HashMap<String,Integer> hhm = new HashMap<String,Integer>();
		hhm = gsn.run();
		
		for(Entry<String,Integer> entry : hm.entrySet()){
			System.out.println("key = "+entry.getKey()+"            values = "+entry.getValue());
		}
		System.out.println("---------------------- ");
	}
	
}
