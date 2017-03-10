import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetSpout extends BaseRichSpout {

    // Twitter API authentication credentials
    private String custkey, custsecret;
    private String accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    TwitterStream twitterStream;
    LinkedBlockingQueue<String> queue = null;


    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status) {
            // add the tweet into the queue buffer
            String geoInfo = "37.7833,122.4167";
            String urlInfo = "n/a";
            if (status.getGeoLocation() != null) {
                geoInfo = String.valueOf(status.getGeoLocation().getLatitude()) + "," + String.valueOf(status.getGeoLocation().getLongitude());
                if (status.getURLEntities().length > 0) {
                    for (URLEntity urlE : status.getURLEntities()) {
                        urlInfo = urlE.getURL();
                    }
                }
                queue.offer(status.getText() + "DELIMITER" + geoInfo + "DELIMITER" + urlInfo);
            }
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

    public TweetSpout(String key, String secret, String token, String tokensecret) {
        custkey = key;
        custsecret = secret;
        accesstoken = token;
        accesssecret = tokensecret;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
// create the buffer to block tweets
        queue = new LinkedBlockingQueue<String>(1000);

        // save the output collector for emitting tuples
        this.collector = collector;


        // build the config with credentials for twitter 4j
        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(custkey)
                        .setOAuthConsumerSecret(custsecret)
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret);

        // create the twitter stream factory with the config
        TwitterStreamFactory fact =
                new TwitterStreamFactory(config.build());

        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        FilterQuery tweetFilterQuery = new FilterQuery(); // See
        tweetFilterQuery.locations(new double[][]{new double[]{-124.848974,24.396308},
                new double[]{-66.885444,49.384358
                }});
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
        String ret = queue.poll();
        String geoInfo;
        String originalTweet;
        // if no tweet is available, wait for 50 ms and return
        if (ret==null)
        {
            Utils.sleep(50);
            return;
        }
        else
        {
            geoInfo = ret.split("DELIMITER")[1];
            originalTweet = ret.split("DELIMITER")[0];
        }

//        if(geoInfo != null && !geoInfo.equals("n/a"))
//        {
//            System.out.print("\t DEBUG SPOUT: BEFORE SENTIMENT \n");
////            int sentiment = SentimentAnalyzer.findSentiment(originalTweet)-2;
//            System.out.print("\t DEBUG SPOUT: AFTER SENTIMENT (" + String.valueOf(sentiment) + ") for \t" + originalTweet + "\n");
//            collector.emit(new Values(ret, sentiment));
//        }
    }

    @Override
    public void close() {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //TODO
    }
}
