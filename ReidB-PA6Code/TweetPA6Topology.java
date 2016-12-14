package heron.starter;

import java.util.*;
import java.util.Map.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import heron.starter.spout.TwitterSpoutPA6;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class TweetPA6Topology {

  private TweetPA6Topology() { }

  /**
   * A spout that emits a random word
   */
  static class WordSpout extends BaseRichSpout {
    private Random rnd;
    private SpoutOutputCollector collector;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
      rnd = new Random(31);
      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      String[] list = {"Jack", "Mary", "Jill", "McDonald"};
      Utils.sleep(10);
      int nextInt = rnd.nextInt(list.length);
      collector.emit(new Values(list[nextInt]));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  static class ConsumerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> countMap;
    private int tupleCount;
    private String taskName;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
      countMap = new HashMap<String, Integer>();
      tupleCount = 0;
      taskName = topologyContext.getThisComponentId() + "_" + topologyContext.getThisTaskId();
    }

	int batchIntervalInSec = 10;
	long lastBatchProcessTimeSeconds = 0;

    @Override
    public void execute(Tuple tuple) {
      String key = tuple.getString(0);
      
      String[] keyList = key.split("\\s+"); 
	List<String> stopWordList = Arrays.asList("a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves", "rt", "&", "-", "—", "~");
	if ((System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds) >= batchIntervalInSec) {
		lastBatchProcessTimeSeconds = System.currentTimeMillis() / 1000;
		System.out.println("~~~~~TWEETS~~~~~");
		Map<String, Integer> map = sortByComparator(countMap, false);
		for (String wrd : map.keySet()){
			Integer count = map.get(wrd);
			if (count > 5) {
				System.out.println(wrd + " " + count);
			}
			//collector.emit(new Values(wrd, count));
		}
		countMap.clear();
    	} else {
		for (String word : keyList){
			if (!stopWordList.contains(word.toLowerCase())){
				String strippedWord = word.replaceAll("[\n\r]","");
				Integer cnt = countMap.get(strippedWord);
				if (cnt == null)
					cnt = 0;
				cnt++;
				countMap.put(strippedWord, cnt);
				collector.ack(tuple);
			}
		}
	}
    }




	private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order)
    {

        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    //builder.setSpout("word", new TwitterSampleSpout(), 2); 
    builder.setSpout("word",
	   new TwitterSpoutPA6("bhG5XahluUvzXeRAptGp8fSF9",
		 "EYL3aKPCtpKclnHXjEBcvZblz8yjsCEklKNMuw96dWGC363B2d",
		 "770354994096791552-UCsWrCT8NlGCKHHuSTqvSV0OHe9MUuB",
		 "P6lSAyCgD93odeu8RzyFQP27p0aiEHRxRqvb6wnwXWDT6"), 1);


    builder.setBolt("count", new ConsumerBolt(), 3).fieldsGrouping("word", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
