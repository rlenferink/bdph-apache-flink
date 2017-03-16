import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(HashTagParseBolt.class);

    // Twitter API authentication credentials
    private String consumerKey, consumerSecret;
    private String accessToken, accessSecret;
    private String[] topics;

    // To output tuples from spout to the next stage bolt
    private SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    private TwitterStream twitterStream;
    private LinkedBlockingQueue<String> queue = null;


    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status) {
            queue.offer(status.getText());
       }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {}

        @Override
        public void onStallWarning(StallWarning warning) {}

        @Override
        public void onException(Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    TweetSpout(String consumerKey, String consumerSecret, String accessToken, String accessSecret, String... topics) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessSecret = accessSecret;
        this.topics = topics;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // create the buffer to block tweets
        queue = new LinkedBlockingQueue<>(1000);

        // save the output collector for emitting tuples
        this.collector = collector;


        // build the config with credentials for twitter 4j
        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(consumerKey)
                        .setOAuthConsumerSecret(consumerSecret)
                        .setOAuthAccessToken(accessToken)
                        .setOAuthAccessTokenSecret(accessSecret);


        // create the twitter stream factory with the config
        TwitterStreamFactory fact = new TwitterStreamFactory(config.build());

        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.language("nl");
        tweetFilterQuery.track(topics);

        // provide the handler for twitter stream
        twitterStream.addListener(new TweetListener());

        twitterStream.filter(tweetFilterQuery);

    }

    @Override
    public void nextTuple() {
        String rawTweet = queue.poll();

        if (rawTweet==null) {
            Utils.sleep(50);
        }
        else {
            collector.emit(new Values(rawTweet));
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_text"));
    }
}
