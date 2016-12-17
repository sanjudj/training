package com.training;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class mapJoin {
	
	public static final String filePath = "/trainingHadoop/userlog/user.log";
	
	public static class mapJoinMapper extends Mapper<LongWritable,Text,Text,Text>{
		Map<String, String> user = null;
		
		public void setup(Context context) throws IOException{
			
			loadKeys(context);
		}
		
		void loadKeys(Context context) throws IOException{
			
			FSDataInputStream in = null;
			BufferedReader br = null;
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = new Path(filePath);
			in = fs.open(path);
			br = new BufferedReader(new InputStreamReader(in));
			user = new HashMap<String, String>();
			String strLine = "";
			
			String userDetails = "";
			
			while((strLine = br.readLine()) != null){
				
				String[] lineArr = strLine.split("\t");
				String userId = lineArr[0];
				String userName = lineArr[1];
				String userEmail = lineArr[2];
				
				userDetails = userName + '|' + userEmail;
				
				user.put(userId, userDetails);				
				
			}
			
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			
			String[] strLine = value.toString().split("\\t");
			String outRec = "";
			if(user.get(strLine[1]) != null){
				
				String[] ue = user.get((strLine)[1]).toString().split("\\|");
				
			
				outRec = strLine[1] + '\t' + ue[0] + '\t' + strLine[2] + '\t' + strLine[3];
				
			}
			
			context.write(new Text(""), new Text(outRec));
		}
				
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
Configuration conf = new Configuration();
		
		Job job = new Job(conf,"map side join");
		job.setJarByClass(mapJoin.class);
		job.setMapperClass(mapJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0:1);


	}

}
