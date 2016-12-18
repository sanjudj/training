package com.training;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class test {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		int counter = 0;
		BufferedWriter br0 = null;
		File file = new File("F:\\trainingHadoop\\duplicateNumber2.txt");
		br0 = new BufferedWriter(new FileWriter(file));
		
		for(int i = 0; i < 10000; i++){
			
			counter = counter + i;
			br0.write(Integer.toString(counter));
			br0.newLine();
			
		}
		
		br0.close();
		
	}

}
