package com.skm.hellostorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.net.URI;
import java.util.Random;

/**
 * @Author: skm
 * @Date: 2019/5/12 8:40
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class DataBuilder {
    private static String ClusterName = "mycluster";
    private static final String HADOOP_URL = "hdfs://" + ClusterName;
    public static Configuration conf;

    static {
        conf = new Configuration();
        conf.set("fs.defaultFS", HADOOP_URL);
        conf.set("dfs.nameservices", ClusterName);
        conf.set("dfs.ha.namenodes." + ClusterName, "nn1,nn2");
        conf.set("dfs.namenode.rpc-address." + ClusterName + ".nn1", "master:8020");
        conf.set("dfs.namenode.rpc-address." + ClusterName + ".nn2", "slave1:8020");
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider." + ClusterName,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    public static void main(String[] args) throws Exception {
        //将数据写到hadoop分布式文件系统中
        URI uri = new URI("hdfs://mycluster");
        FileSystem fileSystem = FileSystem.get(uri, conf);
        Path path = new Path("/dataBuilder/web.log");
        //创建数据数组，模拟数据
        String webSites[] = {"www.baidu.com", "www.csdn.net", "www.cnblogs.com"};

        String ipCode[] = {"DADASDASASCIGFUVBAS", "VIDNBVIIABHFFAS", "IWHUISABIUDBADSA", "COZIONCZNIDADAS", "OIADHAJSBDUABAD"};

        String data[] = {"2019-05-10", "2019-05-11", "2019-05-12", "2019-05-13"};

        Random random = new Random();

        StringBuffer stringBuffer = new StringBuffer();

        for (int i = 0; i < 10000000; i++) {
            stringBuffer.append(webSites[random.nextInt(2)] + "\t" + ipCode[random.nextInt(5)] + "\t" + data[random.nextInt(4)] + "\n");

        }
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        fsDataOutputStream.write(stringBuffer.toString().getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }


}
