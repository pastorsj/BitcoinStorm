package edu.rosehulman.pastorsj;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {
	
	public static void main(String[] args) throws Exception {

		Config config = new Config();
		
		config.setNumWorkers(20);
	    config.setMaxSpoutPending(5000);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("bitcoin-spout",
				new BitcoinSpount());

		builder.setBolt("bitcoin-analysis-bolt", new BitcoinAnalysisBolt()).shuffleGrouping("bitcoin-spout");

//		final LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("TwitterHashtagStorm", config, builder.createTopology());
//
//		Runtime.getRuntime().addShutdownHook(new Thread() {
//			@Override
//			public void run() {
//				cluster.killTopology("TwitterHashtagStorm");
//				cluster.shutdown();
//			}
//		});
      
      StormSubmitter.submitTopology("BitcoinStorm", config, builder.createTopology());

	}
}