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
 * �ļ�����
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
		System.out.println("�ϴ����");
	}
	
	@Test
	public void listFile() throws Exception {
		//ʵ�ֵݹ���������ص�����
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
		while(iterator.hasNext()) {
			//FileStatus�����࣬�����ļ�����ϸ��Ϣ����������blockLocations���ļ��Ŀ���Ϣ��
			LocatedFileStatus status = iterator.next();
			System.out.println("path="+status.getPath());
			System.out.println("owner="+status.getOwner());
			System.out.println("len="+status.getLen());
			System.out.println("rep="+status.getReplication());
			
			//��ȡ���ļ��Ŀ�����
			BlockLocation[] blockLocations = status.getBlockLocations();
			System.out.println("��ĸ�����" + blockLocations.length);
			for (int i = 0; i < blockLocations.length; i++) {
				System.out.println("names="+Arrays.toString(blockLocations[i].getNames()));
				System.out.println("hosts="+Arrays.toString(blockLocations[i].getHosts()));
				System.out.println("offset="+blockLocations[i].getOffset());
				System.out.println("length="+blockLocations[i].getLength());
			}
			
			System.out.println("===========================================");
		}
	}
	
	//ʹ����ʽ���ݷ���
	@Test
	public void get() throws Exception {
		//��������������ȡhdfs�ļ�ϵͳ��a.txt
		FSDataInputStream in = fs.open(new Path("/a.txt"));
		//�����������д������ϵͳ
		FileOutputStream out = new FileOutputStream(new File("e:\\hadoop\\a.txt"));
		//����
//		IOUtils.copyBytes(in, out, 4096);
		//���ز�������
		//ָ��offset
		in.seek(1);
		//ָ��length
		IOUtils.copyBytes(in, out, 3l, true);
		System.out.println("�������");
	}
	
	//����hadoop-2.7.7-centos-6.7.tar.gz�ļ��ĵڶ���
	@Test
	public void readNumber2Block() throws Exception {
		Path path = new Path("/hadoop-2.7.7-centos-6.7.tar.gz");
		//1����ȡ�ļ�����Ϣ
		FileStatus fileStatus = fs.getFileStatus(path);
		//2������Ϣ
		BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		//3���ж��Ƿ���ڵڶ���
		if(blockLocations.length < 2) {
			throw new RuntimeException("���ļ�û�еڶ�����Ϣ");
		}
		//4����ȡ�ڶ����offset
		//4.1����ȡ�ļ���blockSize
		fileStatus.getBlockSize();
		//4.2����ȡ��һ��ĳ���
		blockLocations[0].getLength();
		//4.3����ȡ�ڶ����offset
		long offset = blockLocations[1].getOffset();
		//5����ȡ�ڶ���ĳ���
		long length = blockLocations[1].getLength();
		//6�����������������ָ��offset��length
		FSDataInputStream in = fs.open(path);
		FileOutputStream out = new FileOutputStream(new File("E:\\a.txt"));
		
		//7��ָ��offset
		in.seek(offset);
		
		//8��ָ��length
		IOUtils.copyBytes(in, out, length, true);
		
		System.out.println("���سɹ�");
	}
	
	@After
	public void after() throws Exception {
		fs.close();
	}
}
