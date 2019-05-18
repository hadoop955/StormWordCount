package com.skm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: skm
 * @Date: 2019/5/17 20:29
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */

public class WordCountBolt2 extends BaseRichBolt {

    private Map<String, Integer> map = new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    //将上一个bolt传过来的数据进行统计
    public void execute(Tuple tuple) {
        String words = tuple.getString(0);
        Integer num = tuple.getInteger(1);
        if (map.containsKey(words)) {
            Integer count = map.get(words);
            map.put(words, num + count);
        } else {
            map.put(words, num);
        }

        System.err.println(Thread.currentThread().getId() + "    word:   " + words + "   num:    " +map.get(words));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //不进行输出
    }
}
