package org;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PPMapper extends Mapper<LongWritable, Text,Text, IntWritable>
{
	private IntWritable outval=new IntWritable(1);
	private Text outkey=new Text();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		String line=value.toString();
		String modline=line.replaceAll("\\s+",",").toLowerCase();
		if(value.toString().contains("-"))
		{
			return;
			//context.write(outkey, outval);
		}
		//outval.
		String record=modline.trim();
		String fields[]=record.split(",");
		if(fields!=null&&fields.length==8)
		{
			//String state=fields[0].toLowerCase();
			String party=fields[3].toLowerCase();
			String v1=fields[4].toLowerCase();
			String v2=fields[5].toLowerCase();
			String v3=fields[6].toLowerCase();
			String v4=fields[7].toLowerCase();
		//}
		//String len=fields.length;
		
		outkey.set(party+"#v1"+v1);
		context.write(outkey, outval);
		outkey.set(party+"#v2"+v2);
		context.write(outkey, outval);
		outkey.set(party+"#v3"+v3);
		context.write(outkey, outval);
		outkey.set(party+"#v4"+v4);
		context.write(outkey, outval);
		outkey.set(party+"#"+"all");
		context.write(outkey, outval);
		}
		}
		
	}


