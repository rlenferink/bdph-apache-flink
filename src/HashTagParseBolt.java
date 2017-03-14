import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashTagParseBolt extends BaseRichBolt{
    private static final Logger LOG = LoggerFactory.getLogger(HashTagParseBolt.class);

    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String tweetText = input.getString(0);
        Pattern p = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        Matcher m = p.matcher(tweetText);
        while(m.find()){
            collector.emit(new Values((String) m.group()));
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }
}
