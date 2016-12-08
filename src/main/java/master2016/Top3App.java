package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Creates the Storm topology
 */
public class Top3App {

	public static void main(String[] args) 
	{
		if(args.length != 4){
			System.out.println("Invalid arguments");
			System.exit(1);
		}

		String[] langTopics = args[0].split(","); // en:house,pl:universidade,ar:carro,es:ordenador
		String kafkaBrokerURl = args[1];
		String topologyName = args[2];
		String folder = args[3];

		int numLanguages = langTopics.length;

		String[] languages = new String[numLanguages];
		String[] topics = new String[numLanguages];
		for (int i = 0; i < numLanguages; i++) {
			String currentLangTopic[] = langTopics[i].split(":");
			languages[i] = currentLangTopic[0];
			topics[i] = currentLangTopic[1];
		}

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweetsConsumer", new TweetsConsumer(kafkaBrokerURl), 3);

		builder.setBolt("tweetsFilter", new TweetsFilter(languages), 3)
		.localOrShuffleGrouping("tweetsConsumer", "tweetsStream");

		builder.setBolt("hashtagCounter", new HashtagCounter(languages, topics))
		.fieldsGrouping("tweetsFilter", new Fields("language"));

		builder.setBolt("saveOutput", new SaveOutput(folder))
		.fieldsGrouping("hashtagCounter", new Fields("windowId"));

		Config conf = new Config();
		conf.setNumWorkers(3);

//		try {
//			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//		} catch (AlreadyAliveException e) {
//			e.printStackTrace();
//		} catch (InvalidTopologyException e) {
//			e.printStackTrace();
//		} catch (AuthorizationException e) {
//			e.printStackTrace();
//		}

				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, conf, builder.createTopology());

		//		Utils.sleep(10000);
		//
		//		cluster.killTopology("Top3App");
		//		cluster.shutdown();
	}
}

