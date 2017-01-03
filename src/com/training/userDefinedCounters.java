package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class userDefinedCounters {
	
	static enum highVol {
		volCounter;
	}
	

	public static class udcMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] line = value.toString().split(",");
			String toKey = line[1];
			long toVal = Long.parseLong(line[6]);
			
			if(toVal > 30000){
				
				context.getCounter(highVol.volCounter).increment(1);
				
				
			}else{
				
			context.write(new Text(toKey), new LongWritable(toVal));
			
			}
		}
				
	}

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String userArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf,"user defined counters");
		job.setJarByClass(userDefinedCounters.class);
		job.setMapperClass(udcMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(userArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(userArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
