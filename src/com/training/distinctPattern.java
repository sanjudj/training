package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class distinctPattern {

	
	public static class distictMapper extends Mapper<LongWritable,Text,LongWritable,NullWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String val = value.toString();
			
			long num = Long.parseLong(val);
			
			context.write(new LongWritable(num), NullWritable.get());
		
			
		}
				
	}
	
/*	public static class distinctReducer extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable>{
		
		public void reduce(LongWritable key,Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
			
			context.write(key, NullWritable.get());
		}
	}*/
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"distinct mapper");
		job.setJarByClass(distinctPattern.class);
		job.setMapperClass(distictMapper.class);
		//job.setReducerClass(distinctReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		

	}

}
