package com.training;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class writeHDFS {

	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		conf.addResource(new Path("F://trainingHadoop//core-site.xml"));
		conf.addResource(new Path("F://trainingHadoop//hdfs-site.xml"));
		
		//String otherArgs[] = new GenericOptionsParser(args).getRemainingArgs();
		
		//String localFile = otherArgs[0];
		
		//String hdfsFile = otherArgs[1];
		
		String localFile = "F://firstClass.txt";
		String hdfsFile = "hdfs://ec2-52-7-109-1.compute-1.amazonaws.com:50070/trainingHadoop1";
		
		FileSystem local = FileSystem.getLocal(conf);
		
		FileSystem hdfs = FileSystem.get(new URI("hdfs://ec2-52-7-109-1.compute-1.amazonaws.com:8020"), conf);
		
		FSDataInputStream in = local.open(new Path(localFile));
		
		FSDataOutputStream out = hdfs.create(new Path(hdfsFile));
		
		byte[] buffer = new byte[256];
		
		while(in.read(buffer) > 0){
			
			out.write(buffer,0,256);
		}
		
		in.close();
		out.close();
					
	}

}
