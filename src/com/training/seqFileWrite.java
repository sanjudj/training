package com.training;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class seqFileWrite {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String line[] = {"2783894,sundar",
				         "3784959,Shankar",
				         "8499596,Mangal",
				         "4377586,Anvitha"};
	//	BufferedReader br = new BufferedReader(new FileReader(file));
		//String line = br.readLine();
		String uri = "hdfs://52.4.131.21:8020/trainingHadoop/seqFile/";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		Path path = new Path(uri);
		LongWritable key = new LongWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
		for(int i = 0;i < line.length;i++){
			
			
			String keyVal[] = line[i].split(",");
			key.set(Long.valueOf(keyVal[0]));
			value.set(keyVal[1]);
			writer.append(key, value);
						
		}
				
	}

}
