package com.skm.hellostorm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @Author: skm
 * @Date: 2019/5/13 16:28
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class WebLogBolt implements IRichBolt {
    private OutputCollector collector = null;
    private String valueString = null;
    private int num;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        //获取传递过来的数据
        valueString = tuple.getStringByField("log");

        //如果传过老的数据不为空，则行数++
        if (valueString != null) {
            num++;
            System.out.println(Thread.currentThread().getId()+"     lines :"+
                    + num + "session_id :" + valueString.split("\t")[1]+ "  num :" + num);
        }
        //应答spout接收成功
        collector.ack(tuple);
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            collector.fail(tuple);
            e.printStackTrace();
        }

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //声明输出字段类型
        outputFieldsDeclarer.declare(new Fields("log"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
