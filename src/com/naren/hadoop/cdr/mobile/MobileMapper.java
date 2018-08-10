package com.naren.hadoop.cdr.mobile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MobileMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");
		//output.collect(new Text(SingleCountryData[7]), one);
		IntWritable one = new IntWritable(0);
		try{
			 one = new IntWritable(Integer.parseInt(SingleCountryData[2]));
		}
		catch(NumberFormatException ne){
			
		}
		output.collect(new Text(SingleCountryData[7]), one);
		
	}
}