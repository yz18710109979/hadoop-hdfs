package com.aura.hdfs.api;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 文件操作
 * @author Yangz
 *
 */
public class HDFSDemo3 {

	FileSystem fs = null;
	
	@Before
	public void before() throws Exception {
		URI uri = new URI("hdfs://hdp01:9000");
		Configuration conf = new Configuration();
		fs = FileSystem.get(uri, conf, "hdp");
	}
	
	@Test
	public void listFile() throws Exception {
		//实现递归操作，返回迭代器
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
		while(iterator.hasNext()) {
			//FileStatus的子类，包含文件的详细信息，包含属性blockLocations（文件的块信息）
			LocatedFileStatus status = iterator.next();
			System.out.println("path="+status.getPath());
			System.out.println("owner="+status.getOwner());
			System.out.println("len="+status.getLen());
			System.out.println("rep="+status.getReplication());
			
			//获取该文件的块详情
			BlockLocation[] blockLocations = status.getBlockLocations();
			System.out.println("块的个数：" + blockLocations.length);
			for (int i = 0; i < blockLocations.length; i++) {
				System.out.println("names="+Arrays.toString(blockLocations[i].getNames()));
				System.out.println("hosts="+Arrays.toString(blockLocations[i].getHosts()));
				System.out.println("offset="+blockLocations[i].getOffset());
				System.out.println("length="+blockLocations[i].getLength());
			}
			
			System.out.println("===========================================");
		}
	}
	
	@After
	public void after() throws Exception {
		fs.close();
	}
}
