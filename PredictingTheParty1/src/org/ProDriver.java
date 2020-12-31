package org;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProDriver
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
		Path path=new Path("prodir/ProbFile1.txt");
		URI uri=path.toUri();
		job.addCacheFile(uri);
		
		job.setJarByClass(ProDriver.class);
		job.setMapperClass(ProMapper.class);
		job.setNumReduceTasks(0);
		//job.setReducerClass(PPReducer.class);
		
		//job.setCombinerClass(WCReducer.class);
		// in map reducer
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("prodir2"));
		FileOutputFormat.setOutputPath(job, new Path ("proout51"));
		
		job.waitForCompletion(true);
	}

}