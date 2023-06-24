package aggregator;

import mmm.Mmm;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink implements SinkFunction<Mmm.MsgBatch> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MySink.class);

    @Override
    public void invoke(Mmm.MsgBatch value, Context context) {
        LOG.info("time:"+ value.getTime());
    }
}

