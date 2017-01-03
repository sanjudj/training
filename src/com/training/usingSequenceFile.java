package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class usingSequenceFile {
	
	public static class usingSequenceFileMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			long addKey = key.get();
			String addvalue = value.toString();
			context.write(new LongWritable(addKey), new Text(addvalue));
		}
	}

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = new Job(conf,"sequence file processing");
		job.setJarByClass(usingSequenceFile.class);
		job.setMapperClass(usingSequenceFileMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}

}
