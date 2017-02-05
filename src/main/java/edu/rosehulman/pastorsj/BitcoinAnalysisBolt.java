package edu.rosehulman.pastorsj;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
	Map<String, Double> priceMap;
	Map<String, Double> priceDifferentialMap;
	Map<String, String> priceDifferentialOutput;
	FileOutputStream fos;
	String outputFile;

	public BitcoinAnalysisBolt(String outputFile) {
		this.outputFile = outputFile;
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
		this.priceMap = new HashMap<String, Double>();
		this.priceDifferentialMap = new HashMap<String, Double>();
		this.priceDifferentialOutput = new HashMap<String, String>();
		
		this.priceMap.put("USDBuy", 0.0);
		this.priceMap.put("GBPBuy", 0.0);
		this.priceMap.put("JPYBuy", 0.0);
		this.priceMap.put("RUBBuy", 0.0);
		this.priceMap.put("KRWBuy", 0.0);
		
		this.priceMap.put("USDSell", Double.MAX_VALUE);
		this.priceMap.put("GBPSell", Double.MAX_VALUE);
		this.priceMap.put("JPYSell", Double.MAX_VALUE);
		this.priceMap.put("RUBSell", Double.MAX_VALUE);
		this.priceMap.put("KRWSell", Double.MAX_VALUE);
		
		this.priceDifferentialMap.put("USDDiff", 0.0);
		this.priceDifferentialMap.put("GBPDiff", 0.0);
		this.priceDifferentialMap.put("JPYDiff", 0.0);
		this.priceDifferentialMap.put("RUBDiff", 0.0);
		this.priceDifferentialMap.put("KRWDiff", 0.0);
		
		this.priceDifferentialOutput.put("USDDiff", "USD Price Differential");
		this.priceDifferentialOutput.put("GBPDiff", "GBP Price Differential");
		this.priceDifferentialOutput.put("JPYDiff", "JPY Price Differential");
		this.priceDifferentialOutput.put("RUBDiff", "RUB Price Differential");
		this.priceDifferentialOutput.put("KRWDiff", "KRW Price Differential");
		
		try {
			File f = new File("./output.txt");
			f.delete();
			this.fos = new FileOutputStream(new File(this.outputFile), false);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		JSONObject priceIndex = new JSONObject((String) input.getValueByField("prices"));
		
		double USDBuy = priceIndex.getJSONObject("USD").getDouble("buy");
		double USDSell = priceIndex.getJSONObject("USD").getDouble("sell");
		double GBPBuy = priceIndex.getJSONObject("GBP").getDouble("buy");
		double GBPSell = priceIndex.getJSONObject("GBP").getDouble("sell");
		double JPYBuy = priceIndex.getJSONObject("JPY").getDouble("buy");
		double JPYSell = priceIndex.getJSONObject("JPY").getDouble("sell");
		double RUBBuy = priceIndex.getJSONObject("RUB").getDouble("buy");
		double RUBSell = priceIndex.getJSONObject("RUB").getDouble("sell");
		double KRWBuy = priceIndex.getJSONObject("KRW").getDouble("buy");
		double KRWSell = priceIndex.getJSONObject("KRW").getDouble("sell");
		
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
		
		this.priceDifferentialMap.put("USDDiff", this.priceMap.get("USDSell") - this.priceMap.get("USDBuy"));
		this.priceDifferentialMap.put("GBPDiff", this.priceMap.get("GBPSell") - this.priceMap.get("GBPBuy"));
		this.priceDifferentialMap.put("JPYDiff", this.priceMap.get("JPYSell") - this.priceMap.get("JPYBuy"));
		this.priceDifferentialMap.put("RUBDiff", this.priceMap.get("RUBSell") - this.priceMap.get("RUBBuy"));
		this.priceDifferentialMap.put("KRWDiff", this.priceMap.get("KRWSell") - this.priceMap.get("KRWBuy"));
		
		StringBuffer sb = new StringBuffer();
		
		for (String key : this.priceDifferentialMap.keySet()) {
			String out = this.priceDifferentialOutput.get(key) + " = " + this.priceDifferentialMap.get(key) + "\n";
			sb.append(out);
		}
		String breakLine = "\n-----------------------------------------\n";
		sb.append(breakLine);
		try {
			this.fos.write(sb.toString().getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.fos.write(breakLine.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			this.fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}