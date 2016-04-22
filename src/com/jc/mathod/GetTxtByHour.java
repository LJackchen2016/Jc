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
 * ��Сʱ�ָ��ļ�
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
		  * ���ö���ļ���� 
		  * */ 
		 private MultipleOutputs mos; 
		 
		 @Override 
		 protected void setup(Context context) throws IOException, InterruptedException {			 
		 	mos=new MultipleOutputs(context);//��ʼ��mos 
		 } 
		 
		 @Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 
			 for(Text t : values){
				 mos.write(key.toString().trim(), new Text(key.toString()), new Text(t));	 
			 }	 
		 }
		 
		 @Override 
		 protected void cleanup(Context context) throws IOException, InterruptedException { 
			 mos.close();//�ͷ���Դ 
		 } 
	 }
	 
	 
	 public void run(Integer w) throws IllegalArgumentException, IOException{
		 //����·��
		 String dst = "hdfs://192.168.192.128:8020/input/tradejia/"+w.toString()+"_tradejia_.txt";
		 //���·���������ǲ����ڵģ����ļ���Ҳ���С�
		 String dstOut = "hdfs://192.168.192.128:8020/output/gettxtbyhour/"+w.toString();
		        
		 //��ȡMapReduceϵͳ������Ϣ
		 Configuration conf = new Configuration();  
		        
		 conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		 conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		 Job job = new Job(conf);        
		     
		 //��24��Сʱ������ļ���
		 for(Integer i1 = 0; i1 < 24; i1++){
		     MultipleOutputs.addNamedOutput(job, i1.toString(), TextOutputFormat.class, Text.class, Text.class);
		 }
		        
		 //jobִ����ҵʱ���������ļ���·��
		 FileInputFormat.addInputPath(job, new Path(dst));
		 FileOutputFormat.setOutputPath(job, new Path(dstOut));
		        
		 //ָ���Զ����Mapper��Reducer��Ϊ�����׶ε���������
		 job.setJarByClass(GetTxtByHour.class);
		 job.setMapperClass(GTMapper.class);
		 job.setCombinerClass(GTReducer.class);
		 job.setReducerClass(GTReducer.class);
		        
		 //���������������Key��Value������
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
