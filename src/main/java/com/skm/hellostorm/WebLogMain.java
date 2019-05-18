package com.skm.hellostorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @Author: skm
 * @Date: 2019/5/13 17:02
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class WebLogMain {
    public static void main(String[] args) {
        //创建拓扑对象
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //设置Spout和bolt
        topologyBuilder.setSpout("webLogSpout",new WebLogSpout(),1);
        topologyBuilder.setBolt("webLogBolt",new WebLogBolt(),1).shuffleGrouping("webLogSpout");

        //配置Worker开启个数
        Config config = new Config();
        config.setNumWorkers(2);
        if (args.length>0){
            try {
                //分布式提价拓扑
                StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            //本地提交拓扑
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("webLogTopology",config,topologyBuilder.createTopology());
        }

    }
}
