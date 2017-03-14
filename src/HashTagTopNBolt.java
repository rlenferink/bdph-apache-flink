import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.Map;

public class HashTagTopNBolt extends BaseRichBolt{
    private final long interval;
    private OutputCollector collector;

    HashTagTopNBolt(long interval) {
        this.interval = interval;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String topNJson = input.getString(0);

        try {
            JSONObject topN = new JSONObject(topNJson);
            // TODO Sorteer het json object, pak de top 10, emit die
            // TODO daarna moet een volgende bolt ze mergen en in redis opslaan, overschrijft telkens de oude waarden.

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
}
