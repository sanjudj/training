package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class combinerCode {
	
	public static class combinerCodeMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String[] strLine = value.toString().split(",");
			String stockCode = strLine[1];
			long stockVolume = Long.parseLong(strLine[6]);
			
			context.write(new Text(stockCode), new LongWritable(stockVolume));
						
		}
	}
	
public static class combinerCodeReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException{
			
			long sum = 0;
			
			for(LongWritable values:value){
				
				sum = sum + values.get();
			}
			
			context.write(key, new LongWritable(sum));
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"find sum of stock volumes");
		job.setJarByClass(combinerCode.class);
		job.setMapperClass(combinerCodeMapper.class);
		job.setCombinerClass(combinerCodeReducer.class);
		job.setReducerClass(combinerCodeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0:1);
		
		
	}

}
