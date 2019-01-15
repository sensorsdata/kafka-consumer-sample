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
    String[] brokerList = new String[] {"10.19.91.39:9092"};// 为内网环境的 kafka broker 的地址
    SensorsSimpleConsumer sensorsSimpleConsumer = new SensorsSimpleConsumer(brokerList);
    sensorsSimpleConsumer.assignByTopic(Collections.singleton("master-test")); // 指定订阅的 topic 名
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
