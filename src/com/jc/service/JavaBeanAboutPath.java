package com.jc.service;

public class JavaBeanAboutPath {
	
	private Integer file;
	private Integer hour;
	private String path = "hdfs://192.168.192.128:8020/output/gettxtbyhour/";
	
	public JavaBeanAboutPath(Integer file, Integer hour){
		this.file = file; this.hour = hour;
	}
	
	public void setFile(Integer f){
		this.file = f;
	}
	
	public void setHour(Integer h){
		this.hour = h;
	}
	
	public String getPath(){
		String p = path + file.toString() + "/" + hour.toString() + "-m-00000";
		return p;
	}
	
}
