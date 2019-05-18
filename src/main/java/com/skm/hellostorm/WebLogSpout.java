package com.skm.hellostorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * @Author: skm
 * @Date: 2019/5/13 15:49
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class WebLogSpout implements IRichSpout {
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

    private SpoutOutputCollector spoutOutputCollector = null;
    private BufferedReader bufferedReader = null;
    private String str = null;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        //首先打开输入的文件
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            Path path = new Path("hdfs://mycluster/dataBuilder/web.log");
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            this.bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        //循环调用的方法
        try {
            while ((str = this.bufferedReader.readLine())!=null){
                spoutOutputCollector.emit(new Values(str));
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //输出字段类型
      outputFieldsDeclarer.declare(new Fields("log"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
