package org;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import javax.security.auth.login.Configuration;

//import org.apache.hadoop.fs.Path;

public class GetProb
{
	String []countKey=new String[18];
	int []countVal=new int[18];
	double []probVal=new double[18];
	public void getStat() throws IOException
	{
	//	String loc="hdfs:/user/ppout20/part-r-00000";
	//	Path fpath=new Path(loc);
		
		BufferedReader bf=new BufferedReader(new FileReader("/home/edureka/part-r-00000"));
		//BufferedReader bf=new BufferedReader(new FileReader("hdfs://localhost:50075/user/edureka/ppout20/part-r-00000"));
		String s="";
		int i=0;
//		String []countKey=new String[18];
//		int []countVal=new int[18];
//		int []probVal=new int[18];
		String regex="([dr]#\\w+)\\t(\\d+)";
		Pattern p=Pattern.compile(regex);
		Matcher m;
		while((s=bf.readLine())!=null && i<=17)
		{
			m=p.matcher(s);
			while(m.find())
			{
				countKey[i]=m.group(1);
				countVal[i]=Integer.parseInt(m.group(2));
				i++;
			}
		}
		probVal[0]=(double)(countVal[0])/(countVal[0]+countVal[9]);
		probVal[9]=(double)(countVal[9])/(countVal[0]+countVal[9]);
		for(i=1;i<9;i++)
		{
			probVal[i]=(double)countVal[i]/countVal[0];
		}
		for(i=10;i<18;i++)
		{
			probVal[i]=(double)countVal[i]/countVal[9];
		}
	}
	public void getProb()
	{
		for(int i=0;i<countKey.length;i++)
		{
			System.out.println(countKey[i]+"="+probVal[i]);
		}
	}
	public void setProbFile() throws IOException
	{
		PrintWriter pw=new PrintWriter(new FileWriter("/home/ProbFile1.txt"));
		for(int i=0;i<18;i++)
		{
			pw.println(countKey[i]+"="+probVal[i]);
		}
		pw.close();
	}
	public static void main(String args[]) throws IOException
	{
		GetProb prob=new GetProb();
		prob.getStat();
		prob.getProb();
		prob.setProbFile();
	}
}
