package com.wsy.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;

public class HDFSApp {
    public static final String HDFS_PATH="hdfs://master1:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;
    @Before
    public void setUp() throws Exception{
        configuration =new Configuration();
        /**
         * 第一个参数：HDFS的URL
         * 第二个参数：客户端指定的配置参数
         * 第三个参数：客户端的身份。即用户名
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
        System.out.println("------setUp--------");
    }

    /**
     * 创建hdfs文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        boolean result = fileSystem.mkdirs(new Path("hdfsapi/test"));
    }

    /**
     * 查看HDFS内容
     * @throws Exception
     */
    @Test
    public void text() throws Exception{
        FSDataInputStream  inputStream = fileSystem.open(new Path("/aa/bb/word.txt"));
        IOUtils.copyBytes(inputStream,System.out,1024);

    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        fsDataOutputStream.writeUTF("hello pk replication default 3");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /**
     * 默认的dfs.replication 是3
     */
    @Test
    public void testReplication(){
        System.out.println(configuration.get("dfs.replication"));
    }

    /**
     * 文件重命名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        Path oldPath =new Path("/hdfsapi/test/a.txt");
        Path newPath =new Path("/hdfsapi/test/c.txt");

        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);

    }

    /**
     * 拷贝本地文件到HDFS文件系统
     */
    @Test
    public void copyFromlocalFile() throws Exception{
        Path oldPath =new Path("E:\\git_repo\\大数据笔记.txt");
        Path newPath =new Path("/hdfsapi/test/");

        fileSystem.copyFromLocalFile(oldPath, newPath);
    }

    /**
     * 拷贝大文件到HDFS文件系统
     */
    @Test
    public void copyFromLocalBigFile()throws Exception{
        InputStream in =new BufferedInputStream(new FileInputStream(new File("E:\\jdk-8u144-linux-x64.tar.gz")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/jdk.tar"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in,out,4096);
    }

    /**
     * 拷贝HDFS文件下载到本地
     * @throws Exception
     */
    @Test
    public void copyToLocalFile()throws Exception{
        Path srcPath =new Path("/aa/bb/word.txt");
        Path destPath =new Path("e://");

        fileSystem.copyToLocalFile(srcPath,destPath);
    }

    /**
     * 查看目标文件夹下的所有文件
     */
    @Test
    public void listFile() throws Exception{
        Path path =new Path("/hdfsapi/test/");
        FileStatus[] status = fileSystem.listStatus(path);
        for (FileStatus file :status){
            String isDir = file.isDirectory()? "文件夹":"文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String hdfsPath = file.getPath().toString();
            System.out.println(isDir+"\t"+permission+"\t"+len+"\t"+replication+"\t"+hdfsPath);

        }
    }

    /**
     *递归查看所有的文件及信息
     */
    @Test
    public void listFileRecursive()throws Exception{
        Path path =new Path("/hdfsapi/");
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
        while(files.hasNext()){
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory()? "文件夹":"文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String hdfsPath = file.getPath().toString();
            System.out.println(isDir+"\t"+permission+"\t"+len+"\t"+replication+"\t"+hdfsPath);
        }
    }

    /**
     * 显示副本信息
     * @throws Exception
     */
    @Test
    public void showBlock() throws Exception{
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/jdk.tar"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for(BlockLocation block:blocks){
            for (String name:block.getNames()){
                System.out.println(name+":"+block.getOffset()+":"+block.getLength()+ Arrays.toString(block.getHosts()));
            }
        }

    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void testDelete()throws Exception{
        //第二个参数考虑需不需要递归删除
        boolean deleteResult = fileSystem.delete(new Path("/hdfsapi/test/jdk.tar"),false);
        System.out.println(deleteResult);
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println("------tearDown----------");
    }

}
