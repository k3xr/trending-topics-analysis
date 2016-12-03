package trendingTopology;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Checks language of the tweet
 */
public class TweetsFilter extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private String[] languages;
	private OutputCollector collector;
	private static final ObjectMapper objectMapper = new ObjectMapper();

	public TweetsFilter(String[] languages) {
		super();
		this.languages = languages;
	}

	private boolean checkLang(String language){
		for (int i = 0; i < languages.length; i++) {
			if(languages[i].equals(language)){
				return true;
			}
		}
		return false;
	}

	@Override
	public void execute(Tuple input) {
		String tweet = 	(String)input.getValueByField("tweet");
		JsonNode root;
		try {
			root = objectMapper.readTree(tweet);
			String timestamp = root.get("timestamp_ms").textValue();
			JsonNode hashtagsNode = root.path("entities").path("hashtags");
			String language = root.path("lang").asText();
			if (checkLang(language) && !hashtagsNode.toString().equals("[]")) {
				for (JsonNode node : hashtagsNode) {
					String hashtag = node.path("text").asText();
					//					System.out.println("Emitting " + timestamp + " " + language + " " + hashtag);
					collector.emit(new Values(timestamp, language, hashtag));
				}				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("hashtagStream", new Fields("timestamp", "language", "hashtag"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}
