package KafkaStormIntegration;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt {
    Map<String, Integer> counters;
    Map<String, String> regenerateWords;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<>();
        this.regenerateWords = new HashMap<>();
        this.collector = collector;
    }

    @Override

    public void execute(Tuple input) {
        String str = input.getString(0);
        String uniqueId = str.split(",")[1];
        str = str.split(",")[0];


        if(!regenerateWords.containsKey(uniqueId)){
            regenerateWords.put(uniqueId, str);
        }
        else {
            String valueOfUniqueId = regenerateWords.get(uniqueId);
            valueOfUniqueId += " " + str;
            regenerateWords.put(uniqueId, valueOfUniqueId);
        }

        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        this.collector.emit(new Values(str, str+" lastName", "Software Engineer"));
    }

    @Override
    public void cleanup() {
        for(Map.Entry<String, Integer> entry:counters.entrySet()){
            System.out.println(entry.getKey()+" : " + entry.getValue());
        }

        System.out.println("\nRegenerated Words are");
        for(Map.Entry<String, String> entry: regenerateWords.entrySet()){
            System.out.println(entry.getKey()+" : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("FirstName", "LastName", "Designation"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
