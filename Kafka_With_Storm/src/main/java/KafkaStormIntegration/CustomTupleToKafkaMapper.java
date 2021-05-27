package KafkaStormIntegration;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CustomTupleToKafkaMapper implements TupleToKafkaMapper {
    List<Object> boltKeyList;
    List<Object> boltValueList;

    public CustomTupleToKafkaMapper(List<Object> boltKeyList, List<Object> boltValueList) {
        this.boltKeyList = boltKeyList;
        this.boltValueList = boltValueList;
    }

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return String.join("," , boltKeyList.stream().toArray(String[]::new));
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        List<Object> values = tuple.getValues().stream()
                .map(object -> Objects.toString(object, null)).collect(Collectors.toList());

        return String.join(",", values.stream().toArray(String[]::new));
    }

}
