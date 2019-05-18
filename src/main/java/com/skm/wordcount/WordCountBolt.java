package com.skm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Author: skm
 * @Date: 2019/5/17 20:22
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector outputCollector = null;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        //获取传过来的一行数据
        String line = tuple.getString(0);
        //将数据截取放在数组中
        String [] words = line.split(" ");

        //将接收到的数据以键值对的形式发射出去 (word,1)
        for (String word : words){
            outputCollector.emit(new Values(word,1));
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","num"));
    }
}
