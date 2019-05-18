package com.skm.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * @Author: skm
 * @Date: 2019/5/17 20:12
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */

/**
 * 定义一个发射数据的水龙头，突突突突发射一波
 */
public class WeblogsSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector = null;
    private BufferedReader bufferedReader = null;
    private String string = null;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        //打开我们需要输入的文件
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\IdeaProjects\\stormAPI\\src\\main\\resources\\test.txt")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        //发出我们模拟的数据,循环调用的方法
        while (true){
            try {
                if ((string=bufferedReader.readLine())!=null){
                   spoutOutputCollector.emit(new Values(string));
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }



    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hadoop"));
    }
}
