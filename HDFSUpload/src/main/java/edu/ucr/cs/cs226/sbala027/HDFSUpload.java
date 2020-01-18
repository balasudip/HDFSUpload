package edu.ucr.cs.cs226.sbala027;

import java.io.*;
import java.util.Random;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSUpload
{
	public static void main(String args[]) throws IOException
	{
		if (args.length <= 0 || args[0] == null || args[1] == null) 
		{
			System.out.println("Input or Ouput path has not been provided");
            		System.exit(0);
        	}

        	Configuration hdfsConf = new Configuration();
        	Path destPath = new Path(args[1]);
        	FileSystem hdfsFileSys = FileSystem.get(hdfsConf);
        
		File file = new File(args[0]);
        	if (!file.exists()) 
		{
            		System.out.println("The input file " + args[0] + " does not exist.");
            		System.exit(0);
        	}

        	if (hdfsFileSys.exists(destPath)) 
		{
            		System.out.println("The output file " + args[1] + " already exists.");
            		System.exit(0);
        	}

        	FSDataOutputStream outputStreamToHDFS = hdfsFileSys.create(destPath);
		
        	InputStream inputStreamToLocalFS = new BufferedInputStream(new FileInputStream(args[0]));   
 
        	HDFSUpload.copying(outputStreamToHDFS,inputStreamToLocalFS);

		inputStreamToLocalFS.close();
        	outputStreamToHDFS.close();

		FSDataInputStream inputStreamToHDFS = hdfsFileSys.open(destPath);

		HDFSUpload.reading(inputStreamToHDFS);
		HDFSUpload.randomAccesses(inputStreamToHDFS);

	        inputStreamToHDFS.close();
	        hdfsFileSys.close();  
	} 

	public static void copying(FSDataOutputStream outputStreamToHDFS, InputStream inputStreamToLocalFS)throws IOException
	{
		byte b[] = new byte[512];
		Long intialTimeForLocalToHDFS = Calendar.getInstance().getTimeInMillis();
        	while (inputStreamToLocalFS.read(b) > 0)
		{
			outputStreamToHDFS.write(b);
        	}    
        	Long finalTimeForLocalToHDFS = Calendar.getInstance().getTimeInMillis();
        	Long timeTakenForLocalToHDFS = finalTimeForLocalToHDFS - intialTimeForLocalToHDFS;
        	System.out.println("Time taken to copy 2GB file from local FS to HDFS is " + timeTakenForLocalToHDFS + " milliseconds.");
			
	}


	public static void reading(FSDataInputStream inputStreamToHDFS)throws IOException
	{
		byte b[] = new byte[512];
        	int numOfBytes = 0;
        	Long intialTimeToReadHDFS = Calendar.getInstance().getTimeInMillis();
        	while (inputStreamToHDFS.read(b) > 0) 
		{
            		// outputStreamToLocalFs.write(b);
        	}
        	Long finalTimeToReadHDFS = Calendar.getInstance().getTimeInMillis();
        	Long timeTakenToReadHDFS = finalTimeToReadHDFS - intialTimeToReadHDFS;
        	System.out.println("Time taken to read 2GB file sequentially is " + timeTakenToReadHDFS + " milliseconds");	
	}

	public static void randomAccesses(FSDataInputStream inputStreamToHDFS)throws IOException
	{
		Random randomize = new Random();
        	byte bx[] = new byte[2048];
        	Long intialTimeForRandomRead = Calendar.getInstance().getTimeInMillis();
        	for (int i = 0; i < 2000; i++) 
		{
            		long randomNumber = (long) randomize.nextInt(200000);
            		inputStreamToHDFS.seek(randomNumber);
            		inputStreamToHDFS.read(bx, 0, 1024); // reading 1KB
        	}
        	Long finalTimeForRandomRead = Calendar.getInstance().getTimeInMillis();
        	Long timeTakenForRandomRead = finalTimeForRandomRead - intialTimeForRandomRead;
        	System.out.println("Time taken inorder to make 2,000 random accesses each of size 1KB is " + timeTakenForRandomRead + " milliseconds.");
	
	}
}
