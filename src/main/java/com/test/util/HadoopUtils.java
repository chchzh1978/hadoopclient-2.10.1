package com.test.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

public class HadoopUtils {

	public static String HADOOP_HOST="10.33.14.148";
	public static int HADOOP_PORT=8088;
	public static String JOBTRACKER=HADOOP_HOST+":"+HADOOP_PORT;
	
	public static long JOB_START_TIME=System.currentTimeMillis();
	public static Configuration conf;
	
	public static Configuration getWindowsMapReduceConfiguration(String jarPath, String dfsHostName) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("yarn.resourcemanager.hostname", HadoopUtils.HADOOP_HOST);
		conf.set("hadoop.user.name", "root");
		if(jarPath != null) {
			conf.set("mapreduce.job.jar", jarPath);
		} else {
			throw(new Exception("jarPath can not be null"));
		}		
		//conf.set("yarn.resourcemanager.address", "${yarn.resourcemanager.hostname}:8088");
		conf.set("fs.default.name", dfsHostName);
		conf.set("mapred.job.tracker", HadoopUtils.JOBTRACKER);
		conf.set("mapred.remote.os", "Linux");
		return conf;
	}
	
	public static void setMapReduceConfiguration() {
		Configuration conf = new Configuration();
		setMapReduceConfiguration(conf);
	}
	
	public static void setMapReduceConfiguration(Configuration inputConf) {
		conf = inputConf;
		//conf1.set("mapred.job.tracker", HadoopUtils.JOBTRACKER);
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", HadoopUtils.HADOOP_HOST);
		conf.set("mapreduce.jobhistory.address", HadoopUtils.HADOOP_HOST + ":10020");
	}
	
	public static Configuration getMapReduceConfiguration() {
		return conf;
	}
	
	public static void printJobInformation() throws IOException {
		printJobInformation(null);
	}
	public static void printJobInformation(Configuration inputConf) throws IOException {
		Configuration localConf = null;
		if(inputConf != null) {
			localConf = inputConf;
		}
		else {
			localConf = conf;
		}
		
		JobClient jobClient = new JobClient(localConf);
		JobStatus[] jobsStatus = jobClient.getAllJobs(); 
		System.out.println(jobsStatus.length);		
		for(JobStatus jobStatus : jobsStatus) {
			JobID jobID = jobStatus.getJobID(); 
			/*System.out.println(jobID);
			System.out.println(jobStatus.getUsername());
			System.out.println(
					new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date(jobStatus.getFinishTime())));*/			
			RunningJob runningJob = jobClient.getJob(jobID);  
			if(runningJob != null) {
				//System.out.println(runningJob.getJobState());
				//System.out.println(runningJob.getJobName());
				float map = runningJob.mapProgress();
				//System.out.println("map=" + map);
				float reduce = runningJob.reduceProgress();
				//System.out.println("reduce="+reduce);
				//System.out.println(runningJob.getFailureInfo());
				runningJob.getCounters();
			}
		}
		jobClient.close();
	}
}
