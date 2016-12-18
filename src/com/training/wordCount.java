package com.training;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;

public class wordCount {
	
   public static class wordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
	   
	   LongWritable value = new LongWritable(1);
	   
	   public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
		   
		   String strLine = value.toString();
		   
		   StringTokenizer tokens = new StringTokenizer(strLine);
		   
		   while(tokens.hasMoreTokens()){
			   
			   String keyWord = tokens.nextToken();
			   
			   context.write(new Text(keyWord), this.value);
			  
		   }
		   
	   }
	   
  	   
   }

   public static class wordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
	   
	   public void reduce(Text key,Iterable<LongWritable> value, Context context) throws IOException, InterruptedException{
		   
		   long sum = 0;
		   
		   for(LongWritable values : value){
			   
			   sum = sum + values.get();
		   }
		   
		   context.write(key, new LongWritable(sum));
	   }
	   
   }
   
   
   public static class wordCountPartitioner extends Partitioner<Text,LongWritable>{
	   
	   public int getPartition(Text key, LongWritable value, int numReducePartitions){
		   
		   String partKey = key.toString();
		   
		  if(numReducePartitions == 0){
			  return 0;
		  }
		   
		  if(partKey.startsWith("A")){
			  
			  return 0;
			  
		  }else if(partKey.startsWith("Z")){
			  
			  return 1 % numReducePartitions;
			  
		  }else if(partKey.startsWith("(")){
			  
			  return 2 % numReducePartitions;
			  
		  }else {
			  
			  return 3 % numReducePartitions;
		  }
		   	   
		   
	   }
   }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = new Job(conf,"word count");
		job.setJarByClass(wordCount.class);
		job.setMapperClass(wordCountMapper.class);
		job.setReducerClass(wordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setPartitionerClass(wordCountPartitioner.class);
	//	job.setNumReduceTasks(4);
		System.exit(job.waitForCompletion(true)?0:1);
		
				
	}

}
