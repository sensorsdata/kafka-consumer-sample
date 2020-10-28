package com.sensorsdata.analytics.demo;

import com.sensorsdata.analytics.SensorsSimpleConsumer;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * @auther Codievilky August
 * @since 2019/1/15
 */
public class ConsumerDemo {
  public static void main(String[] args) {
    // 为内网环境的 kafka broker 的地址
    // 请注意！！！ kafka 最终会使用机器的 hostname 进行连接，因此需要将 hostname 配置到当前机器的 /etc/hosts 中
    String[] brokerList = new String[] {"10.19.91.39:9092"};
    SensorsSimpleConsumer sensorsSimpleConsumer = new SensorsSimpleConsumer(brokerList);
    sensorsSimpleConsumer.assignByTopic(Collections.singleton("event_topic")); // 指定订阅的 topic 名
    List<String> records;
    while (true) {
      records = sensorsSimpleConsumer.pollRecords(1000); // 订阅数据
      if (records.size() > 0) {
        break;  // 直到有数据后，停止订阅
      }
    }
    System.out.println(StringUtils.join(records, '\n'));
  }
}
