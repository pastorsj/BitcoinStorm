package edu.rosehulman.pastorsj;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;

@SuppressWarnings("serial")
public class BitcoinSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String bitcoinAPI = "https://blockchain.info/ticker";

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		HttpRequestFactory requestFactory =  new NetHttpTransport().createRequestFactory();
		GenericUrl url;
		try {
			url = new GenericUrl(new URL(bitcoinAPI));
			HttpRequest request = requestFactory.buildGetRequest(url);
			HttpResponse response = request.execute();
			String responseString = response.parseAsString();
			this.collector.emit(new Values(responseString));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("prices"));
	}

}