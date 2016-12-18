package com.training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class reduceJoin {
	
	
	public static class userMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String[] strLine = value.toString().split("\\t");
			String userId = strLine[0];
			String userName = strLine[1];
			String userEmail = strLine[2];
			
			String toVal = "One" + "|" + userName + '|' + userEmail;
			
			String toKey = userId;
			context.write(new Text(toKey), new Text(toVal));	
						
		}
		
	}
	
    public static class userActivityMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String[] strLine = value.toString().split("\\t");
			String userActivity = strLine[0];
			String userId = strLine[1];
			String toKey = userId;
			String toVal = "Two" + '|' + userActivity;
			
			context.write(new Text(toKey), new Text(toVal));
						
		}
		
	}
    
    public static class reduceJoinReducer extends Reducer<Text,Text,Text,Text>{
    	
    	public void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException{
    		ArrayList<String> Arr1 = new ArrayList<String>();
    		ArrayList<String> Arr2 = new ArrayList<String>();
    		
    		Iterator<Text> itr = value.iterator();
    		Text val;
    		String toVal = "";
    		while(itr.hasNext()){
    			
    			
    			val = itr.next();
    			if(val.toString().split("\\|")[0] == "One"){
    				Arr1.add(val.toString().split("\\|")[1]);
    			}else{
    				
    				Arr2.add(val.toString().split("\\|")[0]);
    			}
    			   		
    	}
    		toVal = Arr1.toString() + '|' + Arr2.toString();
    		context.write(key, new Text(toVal));
    	}
       	
    }
    
    
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Reduce side join");
		job.setJarByClass(reduceJoin.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,userMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,userActivityMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setReducerClass(reduceJoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
					
	}

}
