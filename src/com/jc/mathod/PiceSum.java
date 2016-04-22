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

import com.jc.service.JavaBeanAboutPath;

/**
 * 按不同的小时将消费额相加。
 * @author JackChen
 *
 */
public class PiceSum {
	
	
	static class PiceSumMapper extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
			
			String line = value.toString();
			Integer hour = Integer.parseInt(line.substring(0,2).trim());
			if(hour<10){
				Integer id = Integer.parseInt(line.substring(17,20).trim());
				if(id < 100 && id > 0){
					Integer pice = Integer.parseInt(line.substring(20).trim());
					context.write(new Text(hour.toString()), new Text(pice.toString()));
				} else if(id >= 100){
					Integer pice = Integer.parseInt(line.substring(21).trim());
					context.write(new Text(hour.toString()), new Text(pice.toString()));
				}
			}else{
				Integer id = Integer.parseInt(line.substring(18,21).trim());
				if(id < 100 && id > 0){
					Integer pice = Integer.parseInt(line.substring(21).trim());
					context.write(new Text(hour.toString()), new Text(pice.toString()));
				} else if(id >= 100){
					Integer pice = Integer.parseInt(line.substring(22).trim());
					context.write(new Text(hour.toString()), new Text(pice.toString()));
				}
			}
			
		}
	}
	
	 static class PiceSumReducer extends Reducer<Text, Text, Text, Text> {
		 
		 @Override
		 public void reduce(Text key, Iterable<Text> values,
		         Context context) throws IOException, InterruptedException {
			 Integer sumPice = 0;
			 for(Text t : values){
				  sumPice += Integer.parseInt(t.toString());
			 }
			 context.write(key, new Text(sumPice.toString()));
		 }
	}

	public void run(Integer h) throws IllegalArgumentException, IOException{       

	        Configuration conf = new Configuration();  
	        
	        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

	        Job job = new Job(conf);        
	        
	        for(int i=1; i<=7; i++){
		        JavaBeanAboutPath jbap = new JavaBeanAboutPath(i,h);
		        jbap.setFile(i);
		        String dst = jbap.getPath();
		        MultipleInputs.addInputPath(job, new Path(dst),TextInputFormat.class, PiceSumMapper.class);
		    }
	        
		    String dstOut = "hdfs://192.168.192.128:8020/output/picesum/"+h.toString();
		    FileOutputFormat.setOutputPath(job, new Path(dstOut));
	        
	        job.setJarByClass(PiceSum.class);
	        job.setMapperClass(PiceSumMapper.class);
	        job.setCombinerClass(PiceSumReducer.class);
	        job.setReducerClass(PiceSumReducer.class);
	        
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
		for(int h=1; h<=22; h++){
			PiceSum tc = new PiceSum();
			tc.run(h);
		}
		
	}
	
}
