/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aggregator;

import com.twitter.chill.protobuf.ProtobufSerializer;
import mmm.Mmm;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.addDefaultKryoSerializer(Mmm.MsgBatch.class, ProtobufSerializer.class);

		KafkaSource<Mmm.MsgBatch> source = KafkaSource.<Mmm.MsgBatch>builder()
				.setBootstrapServers("192.168.31.50:9092")
				.setTopics("mmm")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(KafkaRecordDeserializationSchema.of(new MyDeserialization()))
				.build();

		DataStream<Mmm.MsgBatch> input = env.fromSource(source, WatermarkStrategy.noWatermarks(),"kafka-source");

		input.addSink(new MySink());

		env.execute("demo");
	}
}

