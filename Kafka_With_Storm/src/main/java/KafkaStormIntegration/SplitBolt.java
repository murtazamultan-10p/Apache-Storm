package KafkaStormIntegration;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.Random;

public class SplitBolt implements IRichBolt {
    private OutputCollector outputCollector;
    private Integer uniqueIdCounter = 1;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {

        String sentence = input.getString(0);
        String[] words = sentence.split(" ");

        for(String word: words) {
            word = word.trim();

            if(!word.isEmpty()) {
                word = word.toLowerCase();
                outputCollector.emit(new Values(word + ",UniqueIdNumber"+uniqueIdCounter));
            }
        }
        uniqueIdCounter++;
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
