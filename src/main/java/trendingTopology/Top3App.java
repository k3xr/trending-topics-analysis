package trendingTopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Top3App {

	public static void main(String[] args) 
	{
		String[] languages = new String[2];
		languages[0] = "en";
		languages[1] = "es";
		String[] topics = new String[2];
		topics[0] = "Travel";
		topics[1] = "trombaMLG";

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweetsConsumer", new TweetsConsumer());

		builder.setBolt("tweetsFilter", new TweetsFilter(languages))
		.localOrShuffleGrouping("tweetsConsumer", "tweetsStream");

		builder.setBolt("hashtagCounter", new HashtagCounter(languages, topics))
		.localOrShuffleGrouping("tweetsFilter", "hashtagStream");

		Config conf = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Top3App", conf, builder.createTopology());

		//		Utils.sleep(10000);
		//
		//		cluster.killTopology("Top3App");
		//		cluster.shutdown();
	}
}

