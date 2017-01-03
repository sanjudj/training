package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class maxTempSecondarySort extends Configured implements Tool {

	
	public static class maxTempSecondarySortMapper extends Mapper<LongWritable,Text,IntPair,NullWritable>{
		
		IntPair intPair = new IntPair();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] val = value.toString().split(",");
			Integer year = Integer.parseInt(val[0]);
			Integer temp = Integer.parseInt(val[1]);
			intPair.set(year, temp);
			
			context.write(intPair, NullWritable.get());
			
			
		}
		
	}
	
	public static class maxTempSecondarySortReducer extends Reducer<IntPair,NullWritable,IntPair,NullWritable>{
		
		public void reduce(IntPair key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
			
			
			context.write(key, NullWritable.get());
		}
	}
	
	public class maxTempPartitioner extends Partitioner<IntPair,NullWritable>{
		
		public int getPartition(IntPair key, NullWritable value, int numPartitions){
			
			return Math.abs(key.getFirst() * 127) % numPartitions;
						
		}
	}
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"secondary sort");
		job.setJarByClass(maxTempSecondarySort.class);
		job.setMapperClass(maxTempSecondarySortMapper.class);
		job.setReducerClass(maxTempSecondarySortReducer.class);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(NullWritable.class);
		job.setPartitionerClass(maxTempPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run((Tool) new maxTempSecondarySort() , args);
		System.exit(exitCode);
	}

}
