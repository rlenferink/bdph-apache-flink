import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetSpout extends BaseRichSpout {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PrintTweetBolt.class);

    // Twitter API authentication credentials
    private String consumerKey, consumerSecret;
    private String accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    private SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    private TwitterStream twitterStream;
    private LinkedBlockingQueue<Status> queue = null;


    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status) {
            queue.offer(status);
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
        public void onException(Exception ex) {}
    }

    TweetSpout(String consumerKey, String consumerSecret, String accesstoken, String accesssecret) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accesstoken = accesstoken;
        this.accesssecret = accesssecret;
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
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret);


        // create the twitter stream factory with the config
        TwitterStreamFactory fact =
                new TwitterStreamFactory(config.build());


        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        FilterQuery tweetFilterQuery = new FilterQuery(); // See
//        tweetFilterQuery.locations(new double[][]{new double[]{-124.848974,24.396308},
//                new double[]{-66.885444,49.384358
//                }});
        tweetFilterQuery.language(new String[]{"en"});



        // provide the handler for twitter stream
        twitterStream.addListener(new TweetListener());

        twitterStream.filter(tweetFilterQuery);

        // start the sampling of tweets
        twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        // try to pick a tweet from the buffer
        Status ret = queue.poll();

        // if no tweet is available, wait for 50 ms and return
        if (ret==null)
        {
            Utils.sleep(50);
            return;
        }
        else
        {
            collector.emit(new Values(Long.toString(ret.getId()), ret.getQuotedStatus()));
        }


    }

    @Override
    public void close() {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "tweet_body"));
    }
}
