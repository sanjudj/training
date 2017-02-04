package com.training;

import java.io.IOException;
import java.util.Iterator;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;


public class globalSort {
	
	public static class MyMapper extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable>
	{
		LongWritable vword = new LongWritable();
		public void map(Text key, Text value,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			        String[] val = value.toString().split(",");
					vword.set(Long.valueOf(val[5]));
					collector.collect(key, vword);
		}
	}
	
	public static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>
	{
		LongWritable vword = new LongWritable();
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			long sum = 0L;
			while(values.hasNext())
			{
				sum = sum + values.next().get();
			}
			vword.set(sum);
			collector.collect(key, vword);
		}

	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("key.value.separator.in.input.line", ",");
		int numOfReducers = 2;
		Path partitionFile = new Path(otherArgs[2]);
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text,Text>(0.1, 10000, 1);
		
		JobConf job = new JobConf(conf);
		job.setJobName("Finding the Stocks Volume using Old MapReduce API");
		
		job.setJarByClass(globalSort.class);
		
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(numOfReducers);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		TotalOrderPartitioner.setPartitionFile(job, partitionFile);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		
		
		InputSampler.writePartitionFile(job, sampler);
		//new Path(otherArgs[1]).getFileSystem(job).delete(new Path(otherArgs[1]), true);
		JobClient.runJob(job);
	}
}