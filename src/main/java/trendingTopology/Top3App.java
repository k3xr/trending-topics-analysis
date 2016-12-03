package trendingTopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Top3App {

	public static void main(String[] args) 
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweetsConsumer", new TweetsConsumer());

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Top3App", new Config(), builder.createTopology());

		Utils.sleep(10000);

		cluster.killTopology("Top3App");
		cluster.shutdown();
	}
}
