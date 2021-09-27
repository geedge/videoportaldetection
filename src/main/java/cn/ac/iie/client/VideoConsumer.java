package cn.ac.iie.client;

import cn.ac.iie.config.GetConf;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class VideoConsumer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    public static final String groupId="test";
    public static final AtomicBoolean isRunning=new AtomicBoolean(true);
    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void consumeVideo(){

        //配置消费者客户端参数
        Properties properties=initConfig();

        //创建相应的消费者实例
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);

        //订阅主题
        consumer.subscribe(Arrays.asList(GetConf.KAFKA_AV_TOPIC));

        try {
            ObjectMapper objectMapper = new ObjectMapper();         // 放到循环之外，性能会好一点
            // StaticFrequency staticFrequency = new StaticFrequency();
            //拉取消息并消费
            while(isRunning.get()){
                // poll是拉到数据会立刻返回，拉不到数据的话就每隔多少秒再拉一次
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
                // ConsumerRecords<String,String> records=consumer.poll(1000);
                for (ConsumerRecord<String,String> record:records){
                    // System.out.println("topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset());
                    // do something to processor record.
                    if (!isJSONValid(record.value())) {
                        continue;
                    }
                    JsonNode jsonNode = objectMapper.readTree(record.value());
                    //获取client_ip, client_port, found_time, recv_time, server_ip, server_port, url字段值
                    JsonNode client_ip = jsonNode.get("client_ip");
                    String cip = client_ip.asText();
                    JsonNode fount_time = jsonNode.get("found_time");
                    JsonNode recv_time = jsonNode.get("recv_time");
                    JsonNode url = jsonNode.get("url");
                    // TODO: 下面是连接ClickHouse
                    // if (url != null)
                    //    staticFrequency.relationSSL2Redis(url.asText(), cip, fount_time.asLong(), 5);

                    // System.out.println(url.asText());
                     System.out.println(record.value());
                }
                // consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    /**
     * 判断是是否为json 组
     */
    public static boolean isJSONValid(String jsonInString ) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    /**
     * 配置消费者客户端参数
     * */
    private static Properties initConfig() {
        Properties properties=new Properties();

        // properties.put("key.deserializer","org.apache.kafka.common.serialization.Deserializer");
//        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class.getName());
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // properties.put("value.deserializer","org.apache.kafka.common.serialization.Deserializer");
        // properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,Deserializer.class.getName());
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 指定连接Kafka集群所需的broker地址清单,中间用逗号隔开,默认值为""
        // properties.put("bootstrap.server",brokerList);
        properties.put("bootstrap.servers", GetConf.BOOTSTRAP_SERVERS);

        // 消费者所属的消费组的名称,默认值为""
        // properties.put("group.id",groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 每次拉1000条进行处理，默认是500
        properties.put("max.poll.records", 1000);

        // 因为是做测试，所以这里就不自动提交下标了
        properties.put("enable.auto.commit", "false");
//        properties.put("auto.offset.reset", "earliest");

        return properties;
    }
}
