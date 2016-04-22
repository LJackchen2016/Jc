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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 用于计算不同学生的数量。
 * @author JackChen
 *
 */
public class AboutStudentNumber {
	
	
	static class ASMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String k = line.substring(28).trim() +"	"+ line.substring(16,17).trim();
			String v = line.substring(0,6)+line.substring(17,27);
			System.out.println(k+"              "+v);
			context.write(new Text(k), new Text(""));
			
		}
	}
	
	 static class ASReducer extends Reducer<Text, Text, Text, Text> {
		 
		 @Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 Integer sumMan = 0;
			 for(Text t : values){
				 sumMan ++;
			 }
			 System.out.println(sumMan);
			 context.write(key, new Text(sumMan.toString()));
		 }
	}
	
	
	public void run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	        
	     conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	     conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

	     Job job = new Job(conf);        
	     
	     String dst = "hdfs://192.168.192.128:8020/input/account.txt";
		 String dstOut = "hdfs://192.168.192.128:8020/output/aboutstudsentnumber";
		 FileInputFormat.addInputPath(job, new Path(dst));
		 FileOutputFormat.setOutputPath(job, new Path(dstOut));
		 job.setJarByClass(AboutStudentNumber.class);
		 job.setMapperClass(ASMapper.class);
//		 job.setCombinerClass(ASReducer.class);
		 job.setReducerClass(ASReducer.class);
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
	
	
	public static void main(String[] args) throws IllegalArgumentException, IOException{
		
		AboutStudentNumber tc = new AboutStudentNumber();
		tc.run();
		
	}
	
}
