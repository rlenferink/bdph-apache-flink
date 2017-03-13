import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.Map;

/**
 * Created by michael on 13-3-2017.
 */
public class PrintTweetBolt extends BaseRichBolt{
    private static final Logger LOG = LoggerFactory.getLogger(PrintTweetBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String id = input.getString(0);
            String json = input.getString(1);
            if (id != null || json != null || id.length() == 0 || json.length() == 0 ){
                return;
            }

            Status status = TwitterObjectFactory.createStatus(input.getString(1));
            LOG.info("Tweet [" + status.getId() + "]", status.getText());
        } catch (TwitterException e) {
            LOG.error("Tweet ["+ input.getString(0) + "]", e.getMessage());
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "tweet_body"));
    }
}
