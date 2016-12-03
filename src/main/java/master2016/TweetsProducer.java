package master2016;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetsProducer 
{
	public final static String TOPIC_NAME = "Tweets";

	public static void main(String[] args )
	{
		if(args.length != 7){
			System.out.println("Invalid arguments");
			System.exit(1);
		}

		String mode = args[0];
		String apiKey = args[1];
		String apiSecret = args[2];
		String tokenValue = args[3];
		String tokenSecret = args[4];
		String kafkaBrokerURL = args[5];
		String filename = args[6];

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put("acks", "1");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("metadata.broker.list", kafkaBrokerURL);

		final KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);

		if (mode.equals("2")){
			try {
				// Initialize twitter stream
				ConfigurationBuilder cb = new ConfigurationBuilder();
				cb.setJSONStoreEnabled(true);
				cb.setIncludeEntitiesEnabled(true);
				cb.setOAuthAccessToken(tokenValue);
				cb.setOAuthAccessTokenSecret(tokenSecret);
				cb.setOAuthConsumerKey(apiKey);
				cb.setOAuthConsumerSecret(apiSecret);

				final TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

				StatusListener listenerEx = new StatusListener(){
					public void onStatus(Status status) {
						HashtagEntity[] hashtags = status.getHashtagEntities();
						if(hashtags.length > 0){
							String value = TwitterObjectFactory.getRawJSON(status);
							String lang = status.getLang();
							// One topic for all tweets
							prod.send(new ProducerRecord<String, String>(TweetsProducer.TOPIC_NAME, lang, value));
						}						
					}
					public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
					public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
					public void onException(Exception ex) {	ex.printStackTrace(); }
					public void onScrubGeo(long arg0, long arg1) {}
					public void onStallWarning(StallWarning arg0) {}
				};
				twitterStream.addListener(listenerEx);
				twitterStream.sample();

			} catch (Exception e) {
				e.printStackTrace();
				prod.close();
			}

		} else if (mode.equals("1")) {
			// Read tweets from file

		} else {
			System.out.println("Invalid arguments");
			System.exit(1);
		}
	}
}
