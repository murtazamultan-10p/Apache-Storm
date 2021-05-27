package KafkaStormIntegration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import KafkaConfiguration.KafkaConfig;


public class KafkaStormSample {


    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(true);

        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
//      TOPOLOGY_WORKERS: How many worker processes to create for the topology across machines in the cluster
        config.put(Config.TOPOLOGY_WORKERS, 1);


        KafkaConfig kafkaConfig = new KafkaConfig();

        // For kafkaSpout
       SpoutConfig kafkaSpoutConfig = kafkaConfig.getkafkaSpoutConfig();

       // For Kafka Bolt
        KafkaBolt kafkaBolt = kafkaConfig.getKafkaBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");

//      parallelism hint means the initial number of executor (threads) of a component.
//      OR How many executors to spawn per component.
        builder.setBolt("kafka-bolt", kafkaBolt, 1)
                .setNumTasks(2) //How many tasks to create per component.
                .fieldsGrouping("word-counter", new Fields("FirstName", "LastName", "Designation"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStormSample", config, builder.createTopology());

        Thread.sleep(10000);

        cluster.shutdown();
    }
}