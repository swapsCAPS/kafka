/**
 * Demonstration of failing state restoration after Kafka Streams restart.
 * This is by no means meant to be an official Kafka Streams test. Demonstration purposes only
 */

package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeExtractor implements TimestampExtractor {
  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    String str = (String) record.value();
    return Long.parseLong(str.split("\\.")[0]);
  }
}

@Tag("integration")
public class RestartIntegrationTest {
  private static final String key = "key";

  private static Admin admin;
  private static KafkaConsumer<String, String> consumer;
  private static KafkaProducer<String, String> producer;
  private static Properties adminProps = new Properties();
  private static Properties kafkaStreamsProps = new Properties();
  private static Properties producerProps = new Properties();
  private static Properties consumerProps = new Properties();

  private static final List<NewTopic> topics = new ArrayList<>();

  @Container
  private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.1.arm64"));

  @BeforeAll
  static void beforeAll() {
    kafka.start();

    String applicationId = "integration-test-" + Instant.now().toEpochMilli();

    kafkaStreamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    kafkaStreamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    kafkaStreamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    kafkaStreamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "integration_test_group_id");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    adminProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

    admin = Admin.create(adminProps);
    consumer = new KafkaConsumer<>(consumerProps);
    producer = new KafkaProducer<>(producerProps);

    topics.add(new NewTopic("input", 1, (short) 1));
    topics.add(new NewTopic("output", 1, (short) 1));

    admin.createTopics(topics);
  }

  @AfterAll
  static void afterAll() throws Exception {
    producer.close();
    consumer.close();

    List<String> topicNames = topics.stream().map(NewTopic::name).collect(Collectors.toList());

    admin.deleteTopics(TopicCollection.ofTopicNames(topicNames)).all().get();
    admin.close();
  }

  StreamsBuilder getTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    SessionWindows windowedBy = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1));

    // Breaks
    SessionBytesStoreSupplier store = Stores.inMemorySessionStore("le_store", Duration.ofMinutes(2));

    // Works
    // SessionBytesStoreSupplier store = Stores.persistentSessionStore("le_store", Duration.ofMinutes(2));

    Materialized<String, String, SessionStore<Bytes, byte[]>> materialized = Materialized.as(store);

    builder.stream("input",
        Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new TimeExtractor()))
      .peek((k, v) -> System.out.println("input k: " + k + " v: " + v))
      .mapValues(v -> v.split("\\.")[1])
      .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
      .windowedBy(windowedBy)
      .reduce((agg, curr) -> agg + " " + curr, materialized)
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
      .toStream()
      .peek((k, v) -> System.out.println("output k: " + k + " v: " + v))
      .mapValues((k, v) -> k + ": " + v)
      .to("output");

    return builder;
  }

  private String sessionToAggregatedString(List<KeyValue<String, String>> messages) {
    StringBuilder stringBuilder = new StringBuilder();
    for (KeyValue<String, String> message : messages) {
      stringBuilder.append(message.value.split("\\.")[1]).append(" ");
    }
    return stringBuilder.toString().trim();
  }

  private List<ConsumerRecord<String, String>> readUntilTime(
    Consumer<String, String> consumer,
    List<String> topics,
    long listenForSeconds) {
    consumer.subscribe(topics);

    List<ConsumerRecord<String, String>> data = new ArrayList<>();

    Instant stopAt = Instant.now().plusSeconds(listenForSeconds);

    while (Instant.now().isBefore(stopAt)) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      records.forEach(data::add);
    }

    return data;
  }

  List<ConsumerRecord<String, String>> startProcessingAndClose(
    List<KeyValue<String, String>> messages,
    boolean shouldClean) throws InterruptedException {
    StreamsBuilder builder = getTopology();
    Topology topology = builder.build();

    Properties props = new Properties();
    props.putAll(kafkaStreamsProps);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);

    KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

    if (shouldClean) {
      kafkaStreams.cleanUp();
    }

    AtomicBoolean isStreamsStarted = new AtomicBoolean(false);

    kafkaStreams.setStateListener((newState, oldState) -> {
      if (newState == KafkaStreams.State.RUNNING) {
        isStreamsStarted.set(true);
      }
    });

    kafkaStreams.start();

    while (!isStreamsStarted.get()) {
      Thread.sleep(1000);
      System.out.println("waiting");
    }
    System.out.println("started : )");

    for (KeyValue<String, String> msg : messages) {
      producer.send(new ProducerRecord<>("input", msg.key, msg.value));
      Thread.sleep(1);
    }

    List<ConsumerRecord<String, String>> sessions;
    sessions = readUntilTime(
      consumer,
      Arrays.asList("output"),
      15L
    );

    kafkaStreams.close();

    return sessions;
  }

  @Test
  void recoversAfterRestart() throws InterruptedException {
    List<KeyValue<String, String>> session1 = toMsgsList(60, 65, key);

    List<KeyValue<String, String>> session2FirstHalf = toMsgsList(180, 182, key);
    List<KeyValue<String, String>> session2SecondHalf = toMsgsList(183, 185, key);
    ArrayList<KeyValue<String, String>> session2 = new ArrayList<>();
    session2.addAll(session2FirstHalf);
    session2.addAll(session2SecondHalf);

    List<KeyValue<String, String>> session3 = toMsgsList(300, 305, key);

    List<KeyValue<String, String>> closer = toMsgsList(420, 420, key);

    List<KeyValue<String, String>> firstBatch = new ArrayList<>(session1);
    firstBatch.addAll(session2FirstHalf);

    List<KeyValue<String, String>> secondBatch = new ArrayList<>(session2SecondHalf);
    secondBatch.addAll(session3);
    secondBatch.addAll(closer);

    List<ConsumerRecord<String, String>> beforeRestart = startProcessingAndClose(firstBatch, true);
    List<String> beforeRestartValues = new ArrayList<>();
    for (ConsumerRecord<String, String> record : beforeRestart) {
      beforeRestartValues.add(record.value());
    }

    List<ConsumerRecord<String, String>> afterRestart = startProcessingAndClose(secondBatch, false);
    List<String> afterRestartValues = new ArrayList<>();
    for (ConsumerRecord<String, String> record : afterRestart) {
      afterRestartValues.add(record.value());
    }

    List<String> expectedBeforeRestart = new ArrayList<>();
    expectedBeforeRestart.add("[" + key + "@60000/65000]: " + sessionToAggregatedString(session1));

    List<String> expectedAfterRestart = new ArrayList<>();
    expectedAfterRestart.add("[" + key + "@180000/185000]: " + sessionToAggregatedString(session2));
    expectedAfterRestart.add("[" + key + "@300000/305000]: " + sessionToAggregatedString(session3));

    assertEquals(expectedBeforeRestart, beforeRestartValues);
    assertEquals(expectedAfterRestart, afterRestartValues);
  }

  private List<KeyValue<String, String>> toMsgsList(int start, int end, String key) {
    List<KeyValue<String, String>> msgs = new ArrayList<>();
    for (int i = start; i <= end; i++) {
      int value = i * 1000;
      msgs.add(new KeyValue<>(key, value + "." + value));
    }
    return msgs;
  }
}
