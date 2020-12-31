package org;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ProMapper extends Mapper<LongWritable,Text,NullWritable,Text>
{
	Text outVal=new Text();
	NullWritable outKey=NullWritable.get();
	HashMap<String,Double> hm=new HashMap<String,Double>();
	String field[];
	private int times=0;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		//super.setup(context);
		Path path[]=context.getLocalCacheFiles();
		String s;
		if(path!=null&&path.length>0)
		{
			for(Path pth:path)
			{
				BufferedReader br=new BufferedReader(new FileReader(pth.toString()));
				while((s=br.readLine())!=null)
				{
					field=s.trim().split("=");
					hm.put(field[0],Double.parseDouble(field[1]));
				}
			}
		}
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
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
		}
		String record=modline.trim();
		String fields[]=record.split(",");
		
		//String key1="d#v1"+fields[4];
		//String key2="r#v1"+fields[4];
		
		
		if(fields!=null&&fields.length==8)
		{
			String party=fields[3].toLowerCase();
			String v1=fields[4].toLowerCase();
			String v2=fields[5].toLowerCase();
			String v3=fields[6].toLowerCase();
			String v4=fields[7].toLowerCase();
			
			double probR=(hm.get("r#all")*hm.get("r#v1"+v1)*hm.get("r#v2"+v2)*hm.get("r#v3"+v3)*hm.get("r#v4"+v4));
			double probD=(hm.get("d#all")*hm.get("d#v1"+v1)*hm.get("d#v2"+v2)*hm.get("d#v3"+v3)*hm.get("d#v4"+v4));
			if(++times<=10)
			{
				if(probR>probD)
				{
					outVal.set("Party: r"+"    Prob#r: "+Double.toString(probR)+"    Prob#d: "+Double.toString(probD));
					context.write(outKey, outVal);
				}
				else
				{
					outVal.set("Party: d"+"    Prob#r: "+Double.toString(probR)+"    Prob#d: "+Double.toString(probD));
					context.write(outKey, outVal);
				}
			}
		}
	}
	
	
}
