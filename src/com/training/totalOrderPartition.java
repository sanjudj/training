package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.InputSampler;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class totalOrderPartition {
	
	
	public static class totalOrderPartitionMapper extends Mapper <Text,Text,Text,LongWritable>{
		
		LongWritable vword = new LongWritable();
		   
		public void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			
			String[] val = value.toString().split(",");
			vword.set(Long.valueOf(val[5]));
			
			context.write(key,vword);
		}
	}
	
	public static class totalOrderPartitionReducer extends Reducer <Text,LongWritable,Text,LongWritable>{
		
		public void reduce(Text key,Iterable<LongWritable> value, Context context) throws IOException, InterruptedException{
			
			/*long sum = 0;
			for(Text values:value){
				
				sum = sum + values.get();
			}*/
			
			context.write(key, (LongWritable) value);
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", ",");
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf,"total order paritioner");
		job.setNumReduceTasks(2);
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text,Text>(0.1, 10000, 1);
		job.setJarByClass(totalOrderPartition.class);
		job.setMapperClass(totalOrderPartitionMapper.class);
		job.setReducerClass(totalOrderPartitionReducer.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(conf, new Path(otherArgs[2]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		InputSampler.writePartitionFile(job, sampler);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		new Path(otherArgs[1]).getFileSystem(conf).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
