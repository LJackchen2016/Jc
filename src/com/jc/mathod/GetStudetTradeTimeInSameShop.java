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
 * 得到学生在各个商店的消费次数。
 */
public class GetStudetTradeTimeInSameShop {
	
	static class GSTTISSMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");	
			String k = line[0]+"	"+line[1];
			context.write(new Text(k), new Text("1"));
		}
	}
	
	
	 static class GSTTISSReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			 Integer sum = 0;
			
			 while(values.iterator().hasNext()){
				 Integer text = Integer.parseInt(values.iterator().next().toString());
				 sum += text;
			 }
			 context.write(new Text(key), new Text(sum.toString()));	 
		 }
	}
	
	
	public void run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst = "hdfs://192.168.192.128:8020/input/tradejia/"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/GetStudetTradeTimeInSameShop/";
		 
		 
		 FileInputFormat.addInputPath(job, new Path(dst));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(GetStudetTradeTimeInSameShop.class);
	     MultipleInputs.addInputPath(job, new Path(dst),TextInputFormat.class, GSTTISSMapper.class);
		 job.setReducerClass(GSTTISSReducer.class);
		 
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
		
		GetStudetTradeTimeInSameShop gsttiss = new GetStudetTradeTimeInSameShop();
		gsttiss.run();
		
	}
	
}
