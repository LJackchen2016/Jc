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
 * 用于将trade表和merchant表按商家好合并到一起。
 * @author JackChen
 *
 */
public class MoneySumByStore {

	
	static class MSMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String v = null;
			String k = null;
			String[] line = value.toString().split("\t");	
			if(line[0].length() <= 3){
				k = line[2].trim();	v = line[3].trim();	
//				System.out.println("k = "+ k +"	     v = "+v);
			}else{
				k = line[1].trim();	v = line[3].trim()+"	"+line[4].trim();
//				System.out.println("k = "+ k +"	     v = "+v);
			}
			context.write(new Text(k), new Text(v));
			
		}
	}
	
	
	 static class MSReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String storeName = null;//用于几率店名
			Integer moneySum = 0;//用于记录店家的营业额
			//取出学生的信息。
			 while(values.iterator().hasNext()){
				 String t = values.iterator().next().toString().trim();
				 if(t.length() > 12){
					 String[] money = t.split("	");
					 moneySum += Integer.parseInt(money[1]); 
				 }else{
					 storeName = t;
				 }
			 }
			 context.write(new Text(storeName), new Text(moneySum.toString()));
		 }
	}
	
	
	public void run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
//	     String dst1 = "hdfs://192.168.192.128:8020/input/merchant.txt"; 
	     String dst2 = "hdfs://192.168.192.128:8020/input/tradejia/"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/merchantandtrade/all/";
		 
		 
//		 FileInputFormat.addInputPath(job, new Path(dst1));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(MoneySumByStore.class);
//	     MultipleInputs.addInputPath(job, new Path(dst1),TextInputFormat.class,MSMapper.class);
	     MultipleInputs.addInputPath(job, new Path(dst2),TextInputFormat.class,MSMapper.class);
		 job.setReducerClass(MSReducer.class);
		 
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
		
		MoneySumByStore ms = new MoneySumByStore();
		ms.run();

	}
	
	
}
