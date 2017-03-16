import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HashTagCollectorBolt extends BaseRichBolt{

    private static final Logger LOG = LoggerFactory.getLogger(HashTagCollectorBolt.class);

    private final long sendInterval;
    private OutputCollector collector;
    private HashMap<String, LinkedList<Long>> counter;
    private long previous_cleanup;

    private long cleanInterval;

    HashTagCollectorBolt (long cleanInterval, long sendInterval) {
        this.cleanInterval = cleanInterval;
        this.sendInterval = sendInterval;
        this.counter = new HashMap<>();
        this.previous_cleanup = System.currentTimeMillis();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        String hashtag = input.getString(0);

        if (!counter.containsKey(hashtag)) {
            counter.put(hashtag, new LinkedList<>());
        }
        counter.get(hashtag).add(System.currentTimeMillis());

        // If the send interval has passed, do a cleanup and send
        if (System.currentTimeMillis() - previous_cleanup > sendInterval) {
            LOG.debug("Starting cleanup");
            previous_cleanup = System.currentTimeMillis();

            Iterator<String> iter =  counter.keySet().iterator();
            while (iter.hasNext()) {
                String key = iter.next();
                LinkedList<Long> times = counter.get(key);
                counter.get(key).removeIf(time -> previous_cleanup - time > cleanInterval);

                if (times.size() == 0) {
                    iter.remove();
                }
            }

            String update = "{";
            for (String counterKey : counter.keySet()) {
                update += "\"" + counterKey + "\": \"" + counter.get(counterKey).size() + "\", ";
            }
            update += "}";
            collector.emit(new Values(update));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag_json"));
    }

}
