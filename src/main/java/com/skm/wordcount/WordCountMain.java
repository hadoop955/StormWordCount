package com.skm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @Author: skm
 * @Date: 2019/5/18 9:08
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class WordCountMain {
    public static void main(String[] args) {
        //创建拓扑对象
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //将spout即数据源头绑定在拓扑上,并且设置并行度为1
        topologyBuilder.setSpout("WeblogsSpout",new WeblogsSpout(),1);
        //将两个处理数据的bolt按照顺序绑定到拓扑上，并且设置各自的并行度,设置分组策略为随机轮询
        topologyBuilder.setBolt("WordCountBolt",new WordCountBolt(),1).shuffleGrouping("WeblogsSpout");
        topologyBuilder.setBolt("WordCountBolt2",new WordCountBolt2(),1).fieldsGrouping("WordCountBolt",new Fields("word"));

        //创建一个configuration来制定topology需要的worker的数量
        Config config = new Config();
        config.setNumWorkers(2);

        //提交任务
        if (args.length>0){
            //进行分布式任务提交
            try {
                StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            //进行本地提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcountTopology",config,topologyBuilder.createTopology());
        }
    }
}
