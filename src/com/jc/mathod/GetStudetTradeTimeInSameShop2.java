package com.jc.mathod;

import java.io.IOException;
import java.util.ArrayList;

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
 * 根据选出的学生，取出各个学生在不同商店消费的次数。
 * @author JackChen
 *
 */
public class GetStudetTradeTimeInSameShop2 {

	
	static class GSTTISSMapper2 extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().trim().split("\t");	
			String k = line[0];
			String v = "";
			if(line.length == 3){
				v = line[1]+"	"+line[2];
			}
			context.write(new Text(k), new Text(v));
		}
	}
	
	
	 static class GSTTISSReducer2 extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			 int flag = 0; //用于标识是否选择了这位学生 0:没有    1:有
			 ArrayList<String> al = new ArrayList<String>();
			 while(values.iterator().hasNext()){
				 Text text = values.iterator().next();
				 if(text.toString().equals(""))
					 flag = 1;
				 al.add(text.toString());
			 }
			 if(flag == 1){
				 for(int i=0; i<al.size(); i++){
					 String str = al.get(i);
					 if(!str.equals("")){
						 context.write(key, new Text(al.get(i)));
					 }
				 }
			 }
			 
		 }
	}
	
	
	public void run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst1 = "hdfs://192.168.192.128:8020/output/getstudent1/"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/GetStudetTradeTimeInSameShop2/";
		 
		 
		 FileInputFormat.addInputPath(job, new Path(dst1));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(GetStudetTradeTimeInSameShop2.class);
	     MultipleInputs.addInputPath(job, new Path(dst1),TextInputFormat.class, GSTTISSMapper2.class);
		 job.setReducerClass(GSTTISSReducer2.class);
		 
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
		
		GetStudetTradeTimeInSameShop2 gsttiss = new GetStudetTradeTimeInSameShop2();
		gsttiss.run();
	}
	
	
}
