package com.test.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

public class HdfsOperationUtils {
	public static String HADOOP_HDFS_HOST="10.33.14.148";
	public static int HADOOP_HDFS_PORT=9000;
	
	public static String HADOOP_HDFS=HADOOP_HDFS_HOST+":"+HADOOP_HDFS_PORT;
	public static String HDFS_BASE_PATH = "hdfs://"+HADOOP_HDFS+"/";
	
	public static void deleteFiles(String path){
    	File file = new File(path);	
    	deleteFiles(file);
	}
    public static void deleteFiles(File file){
        //Check if file exists.
        if (file == null || !file.exists()){
            System.out.println(
            		"Fail to delete the files, please check the path and retry");
            return;
        }
        //Get all the object of files and paths
        File[] files = file.listFiles();
        //Scan all the files and paths in this path
        for (File f: files){
            //Print the file name
            String name = file.getName();
            System.out.println(name);
            //Path or file
            if (f.isDirectory()){
            	deleteFiles(f);
            }else {
                f.delete();
            }
        }
        //ignore inputted path
        //file.delete();
    }
    
	public static void downloadFiles(String localPath,String hdfsPath) throws IOException  {
		downloadFiles(localPath, hdfsPath, "root");
	}
	/*Only download files, not including directory*/
	public static void downloadFiles(String localPath,String hdfsPath, String userName) throws IOException  {
		String hdfsFilePath=HDFS_BASE_PATH + hdfsPath;
		Configuration conf2 = new Configuration();
		FileSystem fs = null;
		try{
			fs = FileSystem.get(URI.create(HDFS_BASE_PATH), conf2, userName);
			RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path(hdfsFilePath), false);
			while(fileList.hasNext())
			{
				LocatedFileStatus elm = fileList.next();
				if(!elm.isFile()) {
					continue;
				}
				String pathStr = elm.getPath().toString();
				String fileName = pathStr.substring(pathStr.lastIndexOf("/")+1,pathStr.length());
				FSDataInputStream fsdi = fs.open(
						new Path(hdfsFilePath + "/" +fileName));
				OutputStream output = new FileOutputStream(localPath + File.separator + fileName);
				IOUtils.copyBytes(fsdi,output,4096,true);
				fsdi.close();
				output.close();
			}
			System.out.println("Finish to download all the files in the path!"); 
		} catch(Exception e) {
			e.printStackTrace();
		} finally{
			if(fs != null) {
				fs.close();
			}
		}
	}
	
	public static void uploadFiles(String filePath,String hdfsPath)  throws IOException{
		uploadFiles(filePath, hdfsPath, "root");
	}
	/*Only download files, not including directory*/
	public static void uploadFiles(String filePath,String hdfsPath, String userName)  throws IOException{
		Configuration conf = new Configuration();
        FileSystem hdfs = null;
        try{
        	hdfs = FileSystem.get(URI.create(HDFS_BASE_PATH), conf, userName);
	        File rootFilePath = new File(filePath);
	        File[] fileList = rootFilePath.listFiles();
	        for(File oneFile : fileList) {
	        	if(!oneFile.isFile()) {
	        		continue;
	        	}
	        	Path dst = new Path(HDFS_BASE_PATH + hdfsPath+"/"+
	        			oneFile.getPath().substring(oneFile.getPath().lastIndexOf(
	        					File.separator)+1,oneFile.getPath().length()));       
	        	hdfs.copyFromLocalFile(new Path(oneFile.getPath()), dst);
	        }
	        Path hdfsBasePath = new Path(HDFS_BASE_PATH + hdfsPath);
	        FileStatus files[] = hdfs.listStatus(hdfsBasePath);
	        for(int i=0;i<files.length;i++)
	        {
	            System.out.println(files[i].getPath());
	        }
        } catch(Exception e) {
        	e.printStackTrace();
        } finally {
        	if(hdfs != null) {
        		hdfs.close();
        	}
        }
	}

	public static void deleteHdfsFiles(String hdfsPath) throws IOException{
		deleteHdfsFiles(hdfsPath, "root");
	}
	/*Only download files, not including directory*/
	public static void deleteHdfsFiles(String hdfsPath, String userName) throws IOException{
		Configuration conf = new Configuration();
		String hdfsFilePath = HDFS_BASE_PATH + hdfsPath;
		FileSystem fs = null;
		try{
			fs = FileSystem.get(URI.create(HDFS_BASE_PATH), conf, userName);
			RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path(hdfsFilePath), false);
			while(fileList.hasNext())
			{
				LocatedFileStatus elm = fileList.next();
				if(!elm.isFile()) {
					continue;
				}
				if(fs.exists(elm.getPath())) {
					fs.deleteOnExit(elm.getPath());
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(fs != null) {
				fs.close();
			}
		}
		
	}
	
	public static void deleteHdfsFile(String hdfsPath,String fileName) throws IOException{
		deleteHdfsFile(hdfsPath, fileName, "root");
	}
	public static void deleteHdfsFile(String hdfsPath,String fileName, String userName) throws IOException{
		Configuration conf = new Configuration();
		String hdfsOutput =HDFS_BASE_PATH + hdfsPath+"/"+fileName;
		Path path = new Path(hdfsOutput);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(HDFS_BASE_PATH), conf, userName);
			if(fs.exists(path)) {
				fs.deleteOnExit(path);
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(fs != null) {
				fs.close();
			}
		}
	}
	
	public static void uploadFile(String localFile,String hdfsPath) throws IOException{
		uploadFile(localFile, hdfsPath, "");
	}
	/*Copy file from local to remote*/
	public static void uploadFile(String localFile,String hdfsPath,String userName) throws IOException{
	    Configuration conf = new Configuration();
	    String hdfsInput = HDFS_BASE_PATH + hdfsPath+"/";
	    FileSystem fs = null;
	    try{
		    fs = FileSystem.get(URI.create(HDFS_BASE_PATH), conf, userName);
		    fs.copyFromLocalFile(new Path(localFile), 
		    		new Path(hdfsInput+localFile.substring(localFile.lastIndexOf(File.separator)+1,localFile.length())));
	    } catch(Exception e) {
	    	e.printStackTrace();
	    } finally {
	    	if(fs != null) {
	    		fs.close();
	    	}
	    }
	}
	
	/*Copy file from remote to local*/
	public static void downloadFile(String hdfsPath, String fileName, String localPath) throws IllegalArgumentException, IOException{
		downloadFile(hdfsPath, fileName, localPath, "root");
	}
	public static void downloadFile(String hdfsPath, String fileName, String localPath, String userName) throws IllegalArgumentException, IOException{
	    String remoteFile =HDFS_BASE_PATH+hdfsPath + "/" + fileName;
	    Path remotePath = new Path(remoteFile);
	    Configuration conf = new Configuration();
	    FileSystem fs = null;
	    try{
		    FileSystem.get(URI.create(HDFS_BASE_PATH),conf,userName);
		    fs.copyToLocalFile(remotePath, new Path(localPath + File.separator + fileName));
	    } catch(Exception e) {
	    	e.printStackTrace();
	    } finally {
	    	if(fs != null) {
	    	    fs.close();
	    	}
	    }
	}
}
