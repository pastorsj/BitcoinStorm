package edu.rosehulman.pastorsj;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;

@SuppressWarnings("serial")
public class BitcoinAnalysisBolt implements IRichBolt {
	OutputCollector _collector;
	Map<String, Integer> priceMap;

	public BitcoinAnalysisBolt() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		this.priceMap = new HashMap<String, Integer>();
		this.priceMap.put("USDBuy", 0);
		this.priceMap.put("GBPBuy", 0);
		this.priceMap.put("JPYBuy", 0);
		this.priceMap.put("RUBBuy", 0);
		this.priceMap.put("KRWBuy", 0);
		this.priceMap.put("USDSell", Integer.MAX_VALUE);
		this.priceMap.put("GBPSell", Integer.MAX_VALUE);
		this.priceMap.put("JPYSell", Integer.MAX_VALUE);
		this.priceMap.put("RUBSell", Integer.MAX_VALUE);
		this.priceMap.put("KRWSell", Integer.MAX_VALUE);
	}

	@Override
	public void execute(Tuple input) {
		JSONObject priceIndex = new JSONObject((String) input.getValueByField("prices"));
		Integer USDBuy = priceIndex.getJSONObject("USD").getInt("buy");
		Integer USDSell = priceIndex.getJSONObject("USD").getInt("sell");
		Integer GBPBuy = priceIndex.getJSONObject("GBP").getInt("buy");
		Integer GBPSell = priceIndex.getJSONObject("GBP").getInt("sell");
		Integer JPYBuy = priceIndex.getJSONObject("JPY").getInt("buy");
		Integer JPYSell = priceIndex.getJSONObject("JPY").getInt("sell");
		Integer RUBBuy = priceIndex.getJSONObject("RUB").getInt("buy");
		Integer RUBSell = priceIndex.getJSONObject("RUB").getInt("sell");
		Integer KRWBuy = priceIndex.getJSONObject("KRW").getInt("buy");
		Integer KRWSell = priceIndex.getJSONObject("KRW").getInt("sell");
		this.priceMap.put("USDBuy", Math.max(USDBuy, this.priceMap.get("USDBuy")));
		this.priceMap.put("GBPBuy", Math.max(GBPBuy, this.priceMap.get("GBPBuy")));
		this.priceMap.put("JPYBuy", Math.max(JPYBuy, this.priceMap.get("JPYBuy")));
		this.priceMap.put("RUBBuy", Math.max(RUBBuy, this.priceMap.get("RUBBuy")));
		this.priceMap.put("KRWBuy", Math.max(KRWBuy, this.priceMap.get("KRWBuy")));
		this.priceMap.put("USDSell", Math.min(USDSell, this.priceMap.get("USDSell")));
		this.priceMap.put("GBPSell", Math.min(GBPSell, this.priceMap.get("GBPSell")));
		this.priceMap.put("JPYSell", Math.min(JPYSell, this.priceMap.get("JPYSell")));
		this.priceMap.put("RUBSell", Math.min(RUBSell, this.priceMap.get("RUBSell")));
		this.priceMap.put("KRWSell", Math.min(KRWSell, this.priceMap.get("KRWSell")));
		for (String key : this.priceMap.keySet()) {
			System.out.println("Price: " + key + ": " + this.priceMap.get(key));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
}