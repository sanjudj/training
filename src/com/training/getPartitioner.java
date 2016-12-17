package com.training;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class getPartitioner {
	
	public static class getPartitionerMapper extends Mapper{
		
		public void map(){
			
		}
		
	}

	
	public static class getPartitionerReducer extends Reducer{
		
		public void reduce()
		{
			
		}
	}
	
	
	public static class getPartition extends Partitioner<Text,Text>{
		
		public int getParition(Text key, Text value, int numReduceTasks){
			
			String[] str = value.toString().split("\\t");
			int age = Integer.parseInt(str[2]);
			
			if(numReduceTasks == 0){
				
				return 0;
			}
			
			if(age <= 20){
				
				return 0;
			}
			
			if(age > 20 && age <= 30){
				return 1 % numReduceTasks;
			}else
			{
				return 2 % numReduceTasks;
			}
		}

		@Override
		public int getPartition(Text arg0, Text arg1, int arg2) {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		//job.setPartitionerClass(getPartition.class);
	}

}
