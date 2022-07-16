package com.damon.kafka.springboot.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class MyConsumer {

    @KafkaListener(topics = "replica-test", groupId = "MyGroup")
    public void listenGroup(ConsumerRecord<String, String> record, Acknowledgment ack) {//处理单条消息记录
        String value = record.value();
        System.out.println(value);
        System.out.println(record);
        ack.acknowledge();//手动提交offset，如果不提交，消息会出现重复消费
    }


    @KafkaListener(groupId = "testGroup", topicPartitions = {
            @TopicPartition(topic = "topic1", partitions = {"0", "1"}),
            @TopicPartition(topic = "topic2", partitions = "0",
                partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "100"))
    }, concurrency = "3")//同组下的并发消费数，建议小于等于分区总数
    public void listenGroupPro(ConsumerRecord<String, String> record, Acknowledgment ack) {//处理单条消息记录
        String value = record.value();
        System.out.println(value);
        System.out.println(record);
        ack.acknowledge();//手动提交offset，如果不提交，消息会出现重复消费
    }

}
