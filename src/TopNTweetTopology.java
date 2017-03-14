import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TopNTweetTopology
{
    private static final Logger LOG = LoggerFactory.getLogger(TopNTweetTopology.class);

    public static void main(String[] args) throws Exception {
        //Variable TOP_N number of words
        int topN = 10;
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        TweetSpout tweetSpout = new TweetSpout(
                "Vhh9wtoeqzWf08BhwmeUXOTSB",
                "Ou1nHWLvIoxQE56mYDJl5JauwkY2N67NSfQYyO46MYjuYDnKIJ",
                "3221388387-VwffqrtFc3P0fHVDZPL8ZxpCuaSUguw2rbGoz23",
                "7dhijHUDA4BBtnCxAdnQqlLiNPWVF2P3jRMmWTrKW2oJ4",
                "VVD", "PVV", "GroenLinks", "GL", "CDA", "PVDA", "SP",
                "CU", "D66", "SGP", "PvdD", "50plus", "stemmen", "verkiezingen"
        );


        builder.setSpout("TweetSpout", tweetSpout, 1);
        builder.setBolt("HashTagParseBolt", new HashTagParseBolt(), 1).shuffleGrouping("TweetSpout");
        builder.setBolt("HashTagCollectorBolt", new HashTagCollectorBolt(30000, 6000),
                1).shuffleGrouping("HashTagParseBolt");
        builder.setBolt("HashTagTopN", new HashTagTopNBolt(60000), 1)
                .shuffleGrouping("HashTagCollectorBolt").setMaxTaskParallelism(1);


        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            LOG.debug("Sending job to remote cluster");

            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {
            LOG.debug("Starting local cluster");

            if (System.getProperty("os.name").toLowerCase().substring(3).equals("win") && !isAdmin()) {
                System.err.println("You are trying to start a local cluster on windows without administrator rights, " +
                        "restart your IDE as an admin to test the cluster.");
            }


            conf.setMaxTaskParallelism(4);
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("TopNTweets", conf, builder.createTopology());

            Utils.sleep(900000000);

            cluster.killTopology("TopNTweets");
            cluster.shutdown();
        }
    }

    public static boolean isAdmin() {
        String groups[] = (new com.sun.security.auth.module.NTSystem()).getGroupIDs();
        for (String group : groups) {
            if (group.equals("S-1-5-32-544"))
                return true;
        }
        return false;
    }
}