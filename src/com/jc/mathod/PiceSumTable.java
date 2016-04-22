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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.jc.mathod.PiceSum.PiceSumMapper;

public class PiceSumTable {
	
	static class AMapper extends Mapper<LongWritable, Text, Text ,Text> {
	
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			context.write(new Text(""), value);
		}
	}

	 static class AReducer extends Reducer<Text, Text, Text, Text> {
		 
		 @Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 
			 for(Text t : values){
				  context.write(key, t);
			 }
			 
		 }
	}

 
 public void run() throws IllegalArgumentException, IOException{
	
        
      //读取MapReduce系统配置信息
        Configuration conf = new Configuration();  
        
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        Job job = new Job(conf);        
     
        for(Integer i1 = 1; i1 < 23; i1++){
        	//输入路径
            String dst = "hdfs://192.168.192.128:8020/output/picesum/" + i1.toString() + "/part-r-00000";
            System.out.println(dst);
            MultipleInputs.addInputPath(job, new Path(dst),TextInputFormat.class, PiceSumMapper.class);
        }
        
        String dstOut = "hdfs://192.168.192.128:8020/output/picesumTable";
        FileOutputFormat.setOutputPath(job, new Path(dstOut));
        
        job.setJarByClass(PiceSumTable.class);
        job.setMapperClass(AMapper.class);
        job.setCombinerClass(AReducer.class);
        job.setReducerClass(AReducer.class);
        
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
		
			PiceSumTable tc = new PiceSumTable();
			tc.run();
	}
}
