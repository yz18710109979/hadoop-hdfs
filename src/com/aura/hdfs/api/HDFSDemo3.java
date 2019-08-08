package com.aura.hdfs.api;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
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
	public void up() throws Exception {
		Path src = new Path("C:\\hadoop-software\\hadoop-2.7.7-centos-6.7.tar.gz");
		Path dst = new Path("/");
		fs.copyFromLocalFile(src, dst);
		System.out.println("上传完成");
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
	
	//使用流式数据访问
	@Test
	public void get() throws Exception {
		//构建输入流，读取hdfs文件系统的a.txt
		FSDataInputStream in = fs.open(new Path("/a.txt"));
		//构建输出流，写到本地系统
		FileOutputStream out = new FileOutputStream(new File("e:\\hadoop\\a.txt"));
		//工具
//		IOUtils.copyBytes(in, out, 4096);
		//下载部分数据
		//指定offset
		in.seek(1);
		//指定length
		IOUtils.copyBytes(in, out, 3l, true);
		System.out.println("下载完成");
	}
	
	//下载hadoop-2.7.7-centos-6.7.tar.gz文件的第二块
	@Test
	public void readNumber2Block() throws Exception {
		Path path = new Path("/hadoop-2.7.7-centos-6.7.tar.gz");
		//1、获取文件的信息
		FileStatus fileStatus = fs.getFileStatus(path);
		//2、块信息
		BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		//3、判断是否存在第二块
		if(blockLocations.length < 2) {
			throw new RuntimeException("该文件没有第二块信息");
		}
		//4、获取第二块的offset
		//4.1、获取文件的blockSize
		fileStatus.getBlockSize();
		//4.2、获取第一块的长度
		blockLocations[0].getLength();
		//4.3、获取第二块的offset
		long offset = blockLocations[1].getOffset();
		//5、获取第二块的场地
		long length = blockLocations[1].getLength();
		//6、构建输入输出流，指定offset和length
		FSDataInputStream in = fs.open(path);
		FileOutputStream out = new FileOutputStream(new File("E:\\a.txt"));
		
		//7、指定offset
		in.seek(offset);
		
		//8、指定length
		IOUtils.copyBytes(in, out, length, true);
		
		System.out.println("下载成功");
	}
	
	@After
	public void after() throws Exception {
		fs.close();
	}
}
