package KafkaConfiguration;

import KafkaStormIntegration.CustomTupleToKafkaMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaConfig {

    private String zkConnString =  "localhost:2181";
    private final String topicKafkaSpout = "kafka-with-storm";
    private final String topicKafkaBolt = "kafka-with-storm-receiver";
    private String serverStringKafkaBolt = "localhost:9092";

    public SpoutConfig getkafkaSpoutConfig(){
        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topicKafkaSpout, "/" + topicKafkaSpout, UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return kafkaSpoutConfig;
    }

    private Properties getKafkaBoltProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", serverStringKafkaBolt);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public KafkaBolt getKafkaBolt(){

        KafkaBolt kafkaBolt = new KafkaBolt();
        kafkaBolt.withTopicSelector(new DefaultTopicSelector(topicKafkaBolt));
        kafkaBolt.withTupleToKafkaMapper(new CustomTupleToKafkaMapper(getKeyList(), new ArrayList<>()));
        kafkaBolt.withProducerProperties(getKafkaBoltProperties());

        return kafkaBolt;
    }

    private List<Object> getKeyList(){
        List<Object> keyList = new ArrayList<>();
        keyList.add("FirstName");
        keyList.add("LastName");
        keyList.add("Designation");

        return keyList;
    }
}
