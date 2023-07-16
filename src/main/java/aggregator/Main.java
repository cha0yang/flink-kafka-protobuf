package aggregator;

import aggregator.config.AppConfig;
import aggregator.config.Fields;
import aggregator.db.CK;
import com.alibaba.fastjson2.JSON;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.json");
        AppConfig config = JSON.parseObject(stream, AppConfig.class);
        assert config != null;

        CK.Init(config);

        Fields aggregatingField = null;
        for (Fields f : config.getFields()) {
            if (f.getType().equals("aggregating")) aggregatingField = f;
        }
        assert aggregatingField != null;

        Time windowSeconds = Time.of(config.windowSeconds, TimeUnit.SECONDS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers("192.168.31.254:9092")
                .setTopics("otlp_spans")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .build();

        DataStream<byte[]> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        Fields finalAggregatingField = aggregatingField;
        input.flatMap(new FlatMapFunction<byte[], Tuple3<String, Long, Long>>() {
                    @Override
                    public void flatMap(byte[] value, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        TracesData data = TracesData.parseFrom(value);
                        for (int i = 0; i < data.getResourceSpansCount(); i++) {
                            ResourceSpans resourceSpans = data.getResourceSpans(i);

                            String service = "_unknown";
                            for (KeyValue attr : resourceSpans.getResource().getAttributesList()) {
                                if (attr.getKey().equals("service.name")) {
                                    service = attr.getValue().getStringValue();
                                }
                            }

                            for (int j = 0; j < resourceSpans.getScopeSpansCount(); j++) {
                                ScopeSpans scopeSpans = resourceSpans.getScopeSpans(j);
                                for (int k = 0; k < scopeSpans.getSpansCount(); k++) {
                                    HashMap<String, Object> m = new HashMap<>();
                                    m.put("service", service);
                                    m.put("count", 2L);

                                    String json = JSON.toJSONString(m);
                                    DocumentContext dc = JsonPath.parse(json);

                                    Long collectValue = 1L;
                                    String aggregatingFieldPath = finalAggregatingField.getPath();
                                    if (!aggregatingFieldPath.equals("")) {
                                        collectValue = Long.parseLong(dc.read(aggregatingFieldPath).toString());
                                    }

                                    out.collect(new Tuple3<>(json, scopeSpans.getSpans(k).getStartTimeUnixNano() / 1000000, collectValue));
                                }
                            }
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.f1))
                .keyBy(new KeySelector<Tuple3<String, Long, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Long, Long> value) {
                        StringBuilder r = new StringBuilder();
                        DocumentContext dc = JsonPath.parse(value.f0);

                        for (Fields f : config.getFields()) {
                            if (f.getType().equals("grouping"))
                                r.append("_").append(dc.read(f.getPath()).toString());
                        }
                        return r.toString();
                    }
                })
                .window(TumblingEventTimeWindows.of(windowSeconds))
                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) {
                        Tuple3<String, Long, Long> result = value1.copy();

                        switch (finalAggregatingField.getAggregateMethod()) {
                            case "avg":
                            case "sum":
                                result.f2 += value2.f2;
                                break;
                            case "max":
                                result.f2 = Math.max(result.f2, value2.f2);
                                break;
                            case "min":
                                result.f2 = Math.min(result.f2, value2.f2);
                                break;
                        }

                        result.f1 = Math.min(result.f1, value2.f1);

                        return result;
                    }
                }).addSink(new SinkFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public void invoke(Tuple3<String, Long, Long> value, Context context) {
                        LOG.info("{}, time: {}, count: {}", value.f0, format.format(new Date(value.f1)), value.f2);

                        List<Object> data = new ArrayList<>();
                        data.add(LocalDateTime.ofEpochSecond(value.f1 / 1000, (int) ((value.f1 % 1000) * 1000000), ZoneOffset.ofHours(8)));

                        DocumentContext dc = JsonPath.parse(value.f0);

                        for (Fields f : config.getFields()) {
                            if (f.getType().equals("grouping"))
                                data.add(dc.read(f.getPath()).toString());
                        }

                        if (finalAggregatingField.getAggregateMethod().equals("avg")) {
                            data.add(value.f2 / config.getWindowSeconds());
                        } else {
                            data.add(value.f2);
                        }

                        CK.insert(data);
                    }
                });

        env.execute("demo");
    }
}

