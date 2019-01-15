package com.sensorsdata.analytics;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * @auther Codievilky August
 * @since 2019/1/15
 */
public class SensorsSimpleConsumer {
  private static final int TIMEOUT_MILLIS = 10000; // 链接超时时间
  private static final int MAX_RECORDS_IN_ONE_POLL = 1000; // 一次最多拿的数据条数
  private KafkaConsumer<String, String> consumer;

  public SensorsSimpleConsumer(String[] brokerList) {
    consumer = getKafkaConsumer(brokerList);
  }

  /**
   * 通过 kafka 的 broker 列表获取消费者
   *
   * @param brokerList kafka broker 的列表
   */
  private KafkaConsumer<String, String> getKafkaConsumer(String[] brokerList) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StringUtils.join(brokerList, ','));
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, RandomStringUtils.randomAlphabetic(10));
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, RandomStringUtils.randomAlphabetic(10));
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, TIMEOUT_MILLIS);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10 * 1024 * 1024);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_RECORDS_IN_ONE_POLL);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // 可填写两个值 latest 与 earliest。分别代表，最新与最早的数据
    return new KafkaConsumer<>(properties);
  }

  /**
   * 通过指定 topic 的方式来订阅数据，订阅进度会有 kafka 来维护
   *
   * @param topicNames 需要消费的 topic 名称的集合
   */
  public void assignByTopic(Collection<String> topicNames) {
    consumer.subscribe(topicNames);
  }

  /**
   * 通过精确地指定 topic partition 来订阅指定位置的数据
   *
   * @param topicPartitions 可以通过 {@link org.apache.kafka.common.TopicPartition#TopicPartition(String, int)} 来构造一个
   *                        topic partition
   */
  public void assignByTopicPosition(Collection<TopicPartition> topicPartitions) {
    consumer.assign(topicPartitions);
  }

  /**
   * 指定某一个 topic partition 的订阅位置
   *
   * @param topicPartition 要指定的 topic partition
   * @param offset         要进行访问的 offset
   */
  public void resetConsumerPosition(TopicPartition topicPartition, int offset) {
    consumer.seek(topicPartition, offset);
  }

  /**
   * 获取指定超时时间获取数据
   *
   * @param timeoutMillis 超时时间
   */
  public List<String> pollRecords(int timeoutMillis) {
    ConsumerRecords<String, String> consumerRecords = consumer.poll(timeoutMillis);
    ArrayList<String> records = new ArrayList<>();
    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
      records.add(consumerRecord.value());
    }
    return records;
  }
}
