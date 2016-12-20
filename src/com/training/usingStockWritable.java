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


public class usingStockWritable {
	
	public static class usingStockWritableMapper extends Mapper<LongWritable,Text,stockWritable,Text>{
		stockWritable emitKey = new stockWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] strLine = value.toString().split(",");
			
			//String stock = strLine[2] + strLine[3];
			
			emitKey.setOpen(Double.valueOf(strLine[2]));
			emitKey.setHigh(Double.valueOf(strLine[3]));
			context.write(emitKey, value);
		}
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"using custom writable");
		job.setJarByClass(usingStockWritable.class);
		job.setMapperClass(usingStockWritableMapper.class);
		job.setMapOutputKeyClass(stockWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		

		
	}

}
