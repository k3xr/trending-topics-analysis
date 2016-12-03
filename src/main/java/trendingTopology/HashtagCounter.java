package trendingTopology;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class HashtagCounter extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private HashMap<String, Integer>[] tweetCount;
	private String[] topics;
	private String[] languages;

	public HashtagCounter(String[] languages, String[] topics) {
		super();
		this.topics = topics;
		this.languages = languages;
		tweetCount = new HashMap[languages.length];
		for (int i= 0; i < languages.length; i++) {
			tweetCount[i] = new HashMap<String, Integer>();
		}
	}

	@Override
	public void execute(Tuple input) {
		//		String timestamp = input.getString(0);
		System.out.println("ASD");
		String language = input.getString(1);
		String hashtag = input.getString(3);
		System.out.println("Saving hashtag " + hashtag + " language " + language);
		for (int i= 0; i < languages.length; i++) {
			if(languages[i].equals(language)){
				if (topics[i].equals(hashtag)) {
					// Start new window
					Iterator<Entry<String, Integer>> it = tweetCount[i].entrySet().iterator();
					while (it.hasNext()) {
						Entry<String, Integer> pair = it.next();
						System.out.println(pair.getKey() + " = " + pair.getValue());
						collector.emit(new Values(languages[i], pair.getKey(), pair.getValue()));
						System.out.println("Emitting " + languages[i] + " " + pair.getKey() + " " + pair.getValue());
						it.remove(); // avoids a ConcurrentModificationException
					}
					tweetCount[i].clear();
				} else {
					int oldCount = tweetCount[i].get(hashtag);
					tweetCount[i].put(hashtag, oldCount+1);
					System.out.println("Saving hashtag " + hashtag + " value " + oldCount+1);
				}
				break;
			}
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("hashtagCountStream", new Fields("language", "hashtag", "count"));
	}

}