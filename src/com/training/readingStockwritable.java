package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import com.training.stockWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class readingStockwritable {
	
	public static class readingMapper extends Mapper<Text,Text,Text,Text>{
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			
			
			
			//String valOpen = Double.toString(open);
			//String valHigh = Double.toString(high);
			
			String val = value.toString();
			
			
			context.write(key, new Text(val));
			
		}
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", "\\t");
		Job job = new Job(conf,"deserializing stock writable");
		job.setJarByClass(readingStockwritable.class);
		job.setMapperClass(readingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}

}
