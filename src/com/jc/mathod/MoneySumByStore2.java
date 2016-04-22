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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MoneySumByStore2 {

	
	static class MSMapper2 extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String v = null;
			String k = null;
			String[] line = value.toString().split("\t");	
			if(line[line.length-1].length() == 10){
				k = line[2].trim();	v = line[3].trim();	
			}else{
				k = line[2].trim();	v = line[1].trim()+"	"+line[2].trim()+"	"+line[4].trim();
			}
			context.write(new Text(k), new Text(v));
			
		}
	}
	
	
	 static class MSReducer2 extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String storeName = null;//用于几率店名
			Integer moneySum = 0;//用于记录店家的营业额
			//取出学生的信息。
			 while(values.iterator().hasNext()){
				 String t = values.iterator().next().toString().trim();
				 if(t.length() > 12){
					 String[] money = t.split("	");
					 moneySum += Integer.parseInt(money[2]); 
				 }else{
					 storeName = t;
				 }
			 }
			 context.write(new Text(storeName), new Text(moneySum.toString()));
		 }
	}
	
	
	public void run(Integer i) throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst1 = "hdfs://192.168.192.128:8020/input/merchant.txt";
	     MultipleInputs.addInputPath(job, new Path(dst1),TextInputFormat.class,MSMapper2.class);
	     for(Integer k=1; k<=7; k++){
	    	 String dst2 = "hdfs://192.168.192.128:8020/output/gettxtbyhour/"+k.toString()+"/"+i.toString()+"-m-00000"; 
	    	 MultipleInputs.addInputPath(job, new Path(dst2),TextInputFormat.class,MSMapper2.class);
	     }
		 String dstOut = "hdfs://192.168.192.128:8020/output/merchantandtrade/"+i.toString();
		 
		 
//		 FileInputFormat.addInputPath(job, new Path(dst1));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(MoneySumByStore2.class);
	     
		 job.setReducerClass(MSReducer2.class);
		 
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
		
		MoneySumByStore2 ms2 = new MoneySumByStore2();
		for(int i=1; i<=22; i++){
			ms2.run(i);
		}
		

	}

	
	
}
