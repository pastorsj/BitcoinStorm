package edu.rosehulman.pastorsj;


import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


@SuppressWarnings("serial")
public class BitcoinAnalysisBolt extends BaseRichSpout {
   SpoutOutputCollector _collector;
		
   public BitcoinAnalysisBolt() {
      // TODO Auto-generated constructor stub
   }
		
   @Override
   public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
         
   }
			
   @Override
   public void nextTuple() {
   }
			
   @Override
   public void close() {
   }
			
   @Override
   public Map<String, Object> getComponentConfiguration() {
      Config ret = new Config();
      ret.setMaxTaskParallelism(1);
      return ret;
   }
			
   @Override
   public void ack(Object id) {}
			
   @Override
   public void fail(Object id) {}
			
   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
   }
}