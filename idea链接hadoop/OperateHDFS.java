package com.qf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class OperateHDFS {
    public void writeToHDFS() throws IOException {
        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME", "root");
        conf.set("fs.defaultFS", "hdfs://192.168.154.10:9000");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("/1.txt");
        FSDataOutputStream fos = fs.create(p, true, 1024);
        fos.write("hellowword".getBytes());
        fos.close();
    }

    @Test
    public void readHDFSFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.154.10:9000");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("/2.txt");
        FSDataInputStream fis = fs.open(p);
        byte[] buf = new byte[1024];
        int len = 0;
        while ((len = fis.read(buf)) != -1) {
            System.out.println(new String(buf, 0, len));
        }
    }

    @Test
    public void putFile() throws IOException {
        Configuration conf = new Configuration();
        //System.setProperty("HADOOP_USER_NAME", "root");
        conf.set("fs.defaultFS", "hdfs://192.168.154.10:9000");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("/");
        Path p2 = new Path("file:///D:/2.txt");
        fs.copyFromLocalFile(p2, p);
        fs.close();
    }
}
