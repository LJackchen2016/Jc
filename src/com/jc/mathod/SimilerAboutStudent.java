package com.jc.mathod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

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

import com.jc.service.GetStoreNumber;
import com.jc.service.StudentAboutStoreTime;

/**
 * 用欧氏距离计算学生之间的相似度
 * @author JackChen
 *
 */
public class SimilerAboutStudent {
	
	static HashMap<String,Integer> storeId = new HashMap<String,Integer>();
	
	//用于记录每个学生和其在各个商家消费的次数
	static ArrayList<StudentAboutStoreTime> stu2 = new ArrayList<StudentAboutStoreTime>();
	static HashMap<String,StudentAboutStoreTime> stu1 = new HashMap<String,StudentAboutStoreTime>();
	
	static class SASMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");
			String stuName = line[0].trim();
			if(stu1.get(stuName).equals(null)){
				StudentAboutStoreTime sast = new StudentAboutStoreTime();
				int i = storeId.get(line[1]);
				sast.setTime(i, Integer.parseInt(line[2].trim()));
				stu1.put(stuName, sast);
			}else{
				StudentAboutStoreTime sast = stu1.get(stuName);
				int i = storeId.get(line[1]);
				sast.setTime(i, Integer.parseInt(line[2].trim()));
				
				stu1.put(stuName, sast);
			}
			context.write(new Text(""), value);
		}
	}
	
	
	 static class SASReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			 while(values.iterator().hasNext()){
				 Text text = values.iterator().next();
				 context.write(new Text(""), new Text(text));
			 }
			//将stu1复制到stu2中去
			int i=0;
			for(Entry<String,StudentAboutStoreTime> entry1 : stu1.entrySet()){
				String stuName = entry1.getKey();
				StudentAboutStoreTime sast = entry1.getValue();
				sast.setName(stuName);
				stu2.set(i, sast);
				i++;
			}
			for(int k=0; k<stu2.size(); i++){
				String stuName = stu2.get(i).getName();
				System.out.println("stuName = "+stuName);
				ArrayList<Integer> al = stu2.get(i).getTime();
				for(int l=0; i<al.size(); l++){
					System.out.print("l = "+al.get(l)+"  |  ");
				}
				System.out.println();
			}
		 }
	}
	
	
	public void run() throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst = "hdfs://192.168.192.128:8020/output/GetStudetTradeTimeInSameShop2/"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/SimilerAboutStudent/";
		 
		 
		 FileInputFormat.addInputPath(job, new Path(dst));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(SimilerAboutStudent.class);
	     MultipleInputs.addInputPath(job, new Path(dst),TextInputFormat.class, SASMapper.class);
		 job.setReducerClass(SASReducer.class);
		 
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
		
		GetStoreNumber gsn = new GetStoreNumber();
		storeId = gsn.run();
		
//		for(Entry<String,Integer> entry : hhm.entrySet()){
//			System.out.println("key = "+entry.getKey()+"            values = "+entry.getValue());
//		}
		
		
		SimilerAboutStudent sas = new SimilerAboutStudent();
		sas.run();
	}

}
