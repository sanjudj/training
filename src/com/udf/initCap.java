package com.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class initCap extends UDF {
	
	public String evaluate(String name){
		
		if(name == null){
			return null;
		}
		
		String arr[] = name.split("\\s+");
		
		for(int i =0;i<arr.length;i++){
			
			char firstLetter = Character.toUpperCase(arr[i].charAt(0));
			String rest = arr[i].substring(1, arr[i].length());
			name = name + Character.toString(firstLetter) + rest;
		
		}
		
		
		return name;
		
		
	}

	

}
