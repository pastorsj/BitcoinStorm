package edu.rosehulman.pastorsj;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Topology {
	
	public static void main(String[] args) throws Exception {

		if(args.length < 1) { 
			System.err.println("Output location not provided");
			return;
		}
		
		String outputFile = args[0];
		
		Config config = new Config();
		
		config.setNumWorkers(20);
	    config.setMaxSpoutPending(20000);
	    
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("bitcoin-spout",
				new BitcoinSpout());

		builder.setBolt("bitcoin-analysis-bolt", new BitcoinAnalysisBolt(outputFile)).shuffleGrouping("bitcoin-spout");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("BitcoinStorm", config, builder.createTopology());
		
		Utils.sleep(20000);
		cluster.killTopology("BitcoinStorm");
		cluster.shutdown();
      
//      StormSubmitter.submitTopology("BitcoinStorm", config, builder.createTopology());

	}
}