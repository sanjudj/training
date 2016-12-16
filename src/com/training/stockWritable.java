package com.training;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class stockWritable implements Writable {

	double open = 0.0;
	double high = 0.0;
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		this.open = in.readDouble();
		this.high = in.readDouble();
				
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		
		out.writeDouble(open);
		out.writeDouble(high);;
	}

	
	public void setOpen(Double stockOpen)
	{
		open = stockOpen;
	}
	public Double getOpen()
	{
		return open;
	}
	public void setHigh(Double stockHigh)
	{
		high = stockHigh;
	}
	public Double getHigh()
	{
		return high;
	}
}
