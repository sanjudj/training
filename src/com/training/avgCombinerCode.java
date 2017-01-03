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
import org.apache.hadoop.mapreduce.Reducer;

public class avgCombinerCode {
	
	
	public static class avgCombinerCodeMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			
			String[] strLine = value.toString().split(",");
			String stockVol = strLine[6];
			
			context.write(new Text("key"), new Text(stockVol));
	
				
		}
		
	}
	
	public static class avgCombinerCodeCombiner extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			
			long count = 0;
			long sum = 0;
			
			for(Text values:value){
				
			   long vol = Long.parseLong(values.toString());
			   
			   sum = sum + vol;
			   count = count + 1;
				
			}
			
			double avg = sum / count;
			String average = Double.toString(avg);
			
			
			context.write(new Text("A_C"), new Text(average + '_' + Long.toString(count)));
			
	}
		
 }
	
	public static class avgCombinerCodeReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text> value1,Context context) throws IOException, InterruptedException{
			
			double sum = 0;
			long counter = 0;
			
			for(Text values:value1){
				
				String[] avgCount = values.toString().split("_");
				long average = Long.parseLong(avgCount[0]);
				long count = Long.parseLong(avgCount[1]);
				
				double ac = average * count;
				
				sum = sum  + ac;
				
				counter = counter + count;
				
			}
			
		
			double avgValue = (double) (sum / counter);
			
			context.write(new Text("Key"), new Text(Double.toString(avgValue)));
			
			System.out.println("average value" + avgValue);
			
		}
		
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf,"find avg of stock volumes using combiner");
		job.setJarByClass(avgCombinerCode.class);
		job.setMapperClass(avgCombinerCodeMapper.class);
		job.setCombinerClass(avgCombinerCodeCombiner.class);
		job.setReducerClass(avgCombinerCodeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0:1);
	}

}
