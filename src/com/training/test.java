package com.training;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class test {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
	int arr[] = {5,1,4,2,8};
	int n = arr.length;
	int temp = 0;
	
	for(int i = 0; i < n; i++){
		
		for(int j = 1; j < (n - 1); j++){
			
			if(arr[j - 1] > arr[j]){
				
				temp       = arr[j - 1];
				arr[j - 1] = arr[j];
				arr[j]     = temp;
				
			}
		}
		

		
	}
	for(int m = 0; m < n; m++){
		System.out.println(arr[m]);
	}
	
	
	int number = 17654;
	int reverse = 0;
	
	while(number != 0){
		
		System.out.println(reverse * 10);
		System.out.println(number % 10);
		reverse = (reverse * 10) + (number % 10);
		
		number = number / 10;
		System.out.println(number);
	}
		
	
	//System.out.println(reverse);
	}

}
