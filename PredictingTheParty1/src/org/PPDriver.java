package org;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PPDriver
{
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args)
		throws IOException,ClassNotFoundException,IOException, Exception
	{	
		Configuration conf=new Configuration();

		Job job=Job.getInstance(conf);
		
		job.setJarByClass(PPDriver.class);
		job.setMapperClass(PPMapper.class);
		job.setReducerClass(PPReducer.class);
		
		//job.setCombinerClass(WCReducer.class);
		// in map reducer
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("ppdir"));
		FileOutputFormat.setOutputPath(job, new Path ("ppout23"));
		
		job.waitForCompletion(true);
	}

}