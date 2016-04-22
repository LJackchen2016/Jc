package com.jc.mathod;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 从文件中按学号取出学生的消费信息，并且计算在该小时内学生的消费情况。
 * @author JackChen
 *
 */
public class AccountAndTradeSum {

	
	static Integer bknan = 0;		static Integer bknv = 0;//本科男和本科女
	static Integer ssnan = 0;		static Integer ssnv = 0;//硕士男和硕士女
	static Integer bsnan = 0;		static Integer bsnv = 0;//博士男和博士女
	
	static Integer bknanxf = 0;		static Integer bknvxf = 0;//本科男和本科女的消费
	static Integer ssnanxf = 0;		static Integer ssnvxf = 0;//硕士男和硕士女的消费
	static Integer bsnanxf = 0;		static Integer bsnvxf = 0;//博士男和博士女的消费
	
	static class AATSMapper1 extends Mapper<LongWritable, Text, Text ,Text> {
		
		@Override	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String v;
			String k;
			String[] line = value.toString().split("\t");	
			if(line.length == 2){
				v = line[1];
				k = line[0];
			}else{
				v = line[1]+"	"+line[2];
				k = line[0];
			}
			context.write(new Text(k.trim()), new Text(v.trim()));
		}
	}
	
	
	 static class AATSReducer extends Reducer<Text, Text, Text, Text> {
		 
		@Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String stuLei = null;//学生类型
			Integer moneySum = 0;//该学生的总消费额
			Integer stuNum = 0; //该学生消费的次数
			//取出学生的信息。
			 while(values.iterator().hasNext()){
				 String[] t = values.iterator().next().toString().split("\t");
				 if(t.length == 1){
					 stuLei = t[0].trim();
				 }else if(t.length == 2){
					 moneySum +=Integer.parseInt(t[1]);
					 stuNum++;
				 } 
			 }
			 if(stuLei.equals("本科男")){
				 bknan += stuNum;	bknanxf += moneySum;
			 }
			 if(stuLei.equals("本科女")){
				 bknv += stuNum;	bknvxf += moneySum;
			 }
			 if(stuLei.equals("硕士男")){
				 ssnan += stuNum;	ssnanxf += moneySum;
			 }
			 if(stuLei.equals("硕士女")){
				 ssnv += stuNum;	ssnvxf += moneySum;
			 }
			 if(stuLei.equals("博士男")){
				 bsnan += stuNum;	bsnanxf += moneySum;
			 }
			 if(stuLei.equals("博士女")){
				 bsnv += stuNum;	bsnvxf += moneySum;
			 }
				 
		 }
	}
	
	
	public void run(Integer i) throws IllegalArgumentException, IOException{       

	     Configuration conf = new Configuration();  
	     Job job = new Job(conf);        
	     
	     String dst1 = "hdfs://192.168.192.128:8020/output/countandtrade/"+i.toString()+"/part-r-00000"; 
		 String dstOut = "hdfs://192.168.192.128:8020/output/countandtradesum/"+i.toString()+"/";
		 
		 
		 FileInputFormat.addInputPath(job, new Path(dst1));
	     FileOutputFormat.setOutputPath(job, new Path(dstOut));
	     
	     job.setJarByClass(AccountAndTradeSum.class);
	     MultipleInputs.addInputPath(job, new Path(dst1),TextInputFormat.class, AATSMapper1.class);
		 job.setReducerClass(AATSReducer.class);
		 
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
		
		AccountAndTradeSum aats = new AccountAndTradeSum();
		for(int i=1; i<23; i++){
			aats.run(i);
			System.out.println(i);
			System.out.println("bknan = "+bknan+"	"+"bknanxf = "+bknanxf);
			System.out.println("bknv = "+bknv+"	"+"bknvxf = "+bknvxf);
			System.out.println("ssnan = "+ssnan+"	"+"ssnanxf = "+ssnanxf);
			System.out.println("ssnv = "+ssnv+"	"+"ssnvxf = "+ssnvxf);
			System.out.println("bsnan = "+bsnan+"	"+"bsnanxf = "+bsnanxf);
			System.out.println("bsnv = "+bsnv+"	"+"bsnvxf = "+bsnvxf);
			System.out.println("-------------------------------------");
			
			//为了循环输出，在这里进行初始化
			bknan = 0;		bknv = 0;//本科男和本科女
			ssnan = 0;		ssnv = 0;//硕士男和硕士女
			bsnan = 0;		bsnv = 0;//博士男和博士女
			
			bknanxf = 0;		bknvxf = 0;//本科男和本科女的消费
			ssnanxf = 0;		ssnvxf = 0;//硕士男和硕士女的消费
			bsnanxf = 0;		bsnvxf = 0;//博士男和博士女的消费
		}
		
	}
	
}
