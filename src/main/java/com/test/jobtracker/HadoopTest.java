package com.test.jobtracker;

import java.io.IOException;

import com.test.util.HadoopUtils;
import com.test.util.HdfsOperationUtils;

public class HadoopTest {
	
	public static void main(String[] args) {
		try {
			/*hdfs process*/
			String localFilePath="D:\\Code\\hdfs\\input";
			String outputfilePath="D:\\Code\\hdfs\\output";
			String inputHdfsPath = "wordcountinput";
			String outputHdfsPath = "wordcountoutput";

			/*Prepare the data set*/
			try {
				HdfsOperationUtils.deleteFiles(outputfilePath);
				HdfsOperationUtils.deleteHdfsFiles(inputHdfsPath);
				HdfsOperationUtils.uploadFiles(localFilePath, inputHdfsPath);
			} catch (IOException e) {
				e.printStackTrace();
			}

			/*Create and submit the map reduce*/
			System.out.println("Word  init");
			try {
				Word.init(HdfsOperationUtils.HDFS_BASE_PATH + inputHdfsPath, 
						HdfsOperationUtils.HDFS_BASE_PATH + outputHdfsPath,
						HadoopUtils.getWindowsMapReduceConfiguration
						("D:\\Code\\hadoop\\JavaHadoopTest-master\\target\\JavaHadoopTest-0.0.1-SNAPSHOT.jar",
								HdfsOperationUtils.HDFS_BASE_PATH));
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("word finished");
			
			/*Query map reduce state*/
			HadoopUtils.setMapReduceConfiguration();
			HadoopUtils.printJobInformation();
			
			/*Copy out put to local*/
			HdfsOperationUtils.downloadFiles(outputfilePath, outputHdfsPath);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			System.exit(0);
		}
	}
}
