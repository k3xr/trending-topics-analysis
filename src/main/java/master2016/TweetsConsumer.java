package master2016;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Spout that reads tweets from kafka and emits them to the topology
 */
public class TweetsConsumer extends BaseRichSpout{	

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private KafkaConsumer<String, String> kafkaConsumer;
	private String kafkaBrokerURL;

	public TweetsConsumer(String kafkaBrokerURL)
	{
		super();
		this.kafkaBrokerURL = kafkaBrokerURL;
	}

	@Override
	public void nextTuple()
	{        
		ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
		for (ConsumerRecord<String, String> record : records) {
			collector.emit("tweetsStream", new Values(new String(record.value())));
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map arg0, TopologyContext context, SpoutOutputCollector collector)
	{
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerURL);
		props.put("group.id", "MYGROUP");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.kafkaConsumer = new KafkaConsumer<>(props);
		this.collector = collector;
		kafkaConsumer.subscribe(Arrays.asList("Tweets"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declareStream("tweetsStream", new Fields("tweet"));
	}
}
