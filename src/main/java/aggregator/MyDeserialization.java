package aggregator;

import mmm.Mmm;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyDeserialization implements KafkaDeserializationSchema<Mmm.MsgBatch> {

    private static final Logger LOG = LoggerFactory.getLogger(MyDeserialization.class);
    public MyDeserialization() {}

    @Override
    public TypeInformation<Mmm.MsgBatch> getProducedType() {
        return TypeInformation.of(Mmm.MsgBatch.class);
    }

    @Override
    public boolean isEndOfStream(Mmm.MsgBatch nextElement) {
        return false;
    }

    @Override
    public Mmm.MsgBatch deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        if (consumerRecord != null) {
            try {
                Mmm.MsgBatch mb = Mmm.MsgBatch.parseFrom(consumerRecord.value());
                return mb;
            } catch (Exception e) {
                LOG.info("deserialize failed : " + e.getMessage());
            }
        }
        return null;
    }
}
