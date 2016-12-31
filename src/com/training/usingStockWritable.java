package com.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.training.stockWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class usingStockWritable {
	
	public static class usingStockWritableMapper extends Mapper<LongWritable,Text,IntWritable,stockWritable>{
		stockWritable emitValue = new stockWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] strLine = value.toString().split(",");
			
			//String stock = strLine[2] + strLine[3];
			emitValue.setOpen(Double.valueOf(strLine[2]));
			emitValue.setHigh(Double.valueOf(strLine[3]));
			context.write(new IntWritable(Integer.parseInt(strLine[6])), emitValue);
		}
		
	}
	
	public static class usingStockWritableReducer extends Reducer<IntWritable,stockWritable,IntWritable,DoubleWritable>{
		
		public void reduce(IntWritable key,Iterable<stockWritable> value,Context context) throws IOException, InterruptedException{
			
			double stockOpen = 0.0;
			double holdOpen = 0.0;
			double maxOpen = 0.0;
			
			for (stockWritable values : value) {
				
				 stockOpen = values.getOpen();
				 
				 if(stockOpen > holdOpen){
					 
					 maxOpen = stockOpen;
				 }
				 
			     holdOpen = stockOpen; 			
				
			}
			
			context.write(key, new DoubleWritable(maxOpen));
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"using custom writable");
		job.setJarByClass(usingStockWritable.class);
		job.setMapperClass(usingStockWritableMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(stockWritable.class);
		job.setReducerClass(usingStockWritableReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}

}
