package com.jc.service;

import java.util.ArrayList;

public class StudentAboutStoreTime {

	public String name;
	public static ArrayList<Integer> time = new ArrayList<Integer>(85){{add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);
	add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);add(0);}};
	
	public void setTime(int i, int num){
		time.set(i, num);
	}
	
	public void setName(String n){
		name = n;
	}
	
	public ArrayList<Integer> getTime(){
		return time;
	}
	
	public String getName(){
		return name;
	}
	
	public static void main(String[] args) {

		for(int i=0; i<85; i++){
			System.out.println("i = "+i+"      "+time.get(i));
		}
		
	}

}
