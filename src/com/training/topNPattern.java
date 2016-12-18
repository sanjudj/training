package com.training;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class topNPattern {
	
	public static class topNMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
		
		TreeMap<Double,String> hashSet = new TreeMap<Double,String>();
		
		public void map(LongWritable key,Text value,Context context){
			
			String[] val = value.toString().split("\\t");
			String custId = val[0];
			Double clv = Double.parseDouble(val[1]);
			
			hashSet.put(clv, custId);
			
			if(hashSet.size() > 10){
				
				hashSet.remove(hashSet.firstKey());
			}
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			
			//for(String id: hashSet.values()){
				Set<Double> keys = hashSet.keySet();
				for(Double k: keys){	
					
					String clv = k.toString();
					String id = hashSet.get(k);
					String outVal = clv + '|' + id;
				context.write(NullWritable.get(), new Text(outVal));
			}
		}
		
	}

	
//implement reducer to get topn from all the maps	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

	Configuration conf = new Configuration();
	Job job = new Job(conf,"top 10");
	job.setJarByClass(topNPattern.class);
	job.setMapperClass(topNMapper.class);
	job.setMapOutputKeyClass(NullWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		
	}

}
