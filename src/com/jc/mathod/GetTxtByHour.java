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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 按小时分割文件
 * @author JackChen
 *
 */
public class GetTxtByHour {
	
	static class GTMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {

			String line = value.toString();
			
			Integer id = Integer.parseInt(line.substring(15,18).trim());
			if(id < 100 && id > 0){
				Integer hour = Integer.parseInt(line.substring(29,31).trim());
				if(hour>=0&&hour<24){
					String str1 = line.substring(0,17);
					String str2 = line.substring(37);
					String newLine = str1 + str2;
					context.write(new Text(hour.toString()), new Text(newLine));
				}
			} else if(id >= 100){
				Integer hour = Integer.parseInt(line.substring(30,32).trim());
				if(hour>=0&&hour<24){
					String str1 = line.substring(0,18);
					String str2 = line.substring(38);
					String newLine = str1 + str2;
					context.write(new Text(hour.toString()), new Text(newLine));
				}
			}
			
		}
	}
	
	 static class GTReducer extends Reducer<Text, Text, Text, Text> {
		 
		 /** 
		  * 设置多个文件输出 
		  * */ 
		 private MultipleOutputs mos; 
		 
		 @Override 
		 protected void setup(Context context) throws IOException, InterruptedException {			 
		 	mos=new MultipleOutputs(context);//初始化mos 
		 } 
		 
		 @Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 
			 for(Text t : values){
				 mos.write(key.toString().trim(), new Text(key.toString()), new Text(t));	 
			 }	 
		 }
		 
		 @Override 
		 protected void cleanup(Context context) throws IOException, InterruptedException { 
			 mos.close();//释放资源 
		 } 
	 }
	 
	 
	 public void run(Integer w) throws IllegalArgumentException, IOException{
		 //输入路径
		 String dst = "hdfs://192.168.192.128:8020/input/tradejia/"+w.toString()+"_tradejia_.txt";
		 //输出路径，必须是不存在的，空文件加也不行。
		 String dstOut = "hdfs://192.168.192.128:8020/output/gettxtbyhour/"+w.toString();
		        
		 //读取MapReduce系统配置信息
		 Configuration conf = new Configuration();  
		        
		 conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		 conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		 Job job = new Job(conf);        
		     
		 //按24个小时来输出文件。
		 for(Integer i1 = 0; i1 < 24; i1++){
		     MultipleOutputs.addNamedOutput(job, i1.toString(), TextOutputFormat.class, Text.class, Text.class);
		 }
		        
		 //job执行作业时输入和输出文件的路径
		 FileInputFormat.addInputPath(job, new Path(dst));
		 FileOutputFormat.setOutputPath(job, new Path(dstOut));
		        
		 //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
		 job.setJarByClass(GetTxtByHour.class);
		 job.setMapperClass(GTMapper.class);
		 job.setCombinerClass(GTReducer.class);
		 job.setReducerClass(GTReducer.class);
		        
		 //设置最后输出结果的Key和Value的类型
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
			
		for(Integer i = 1; i <= 7; i++){
			GetTxtByHour tc = new GetTxtByHour();
			tc.run(i);
		}
	}
	
}
