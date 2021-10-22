package cn.ac.iie.mining;

import cn.ac.iie.Utils.JedisPoolUtils;
import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.Utils.URLHandleUtils;
import cn.ac.iie.config.GetConf;
import cn.ac.iie.mapper.AVDataListMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ibatis.session.SqlSession;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class MiningServiceTest {
    @Test
    public void consumeKafka() {
        Properties properties=new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", GetConf.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // max.poll.records设置的条数要在一定的时间间隔处理完
        properties.put("max.poll.records", 50);
        // 自动提交
        properties.put("enable.auto.commit", "false");
        // 测试的时候设置为从头开始
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(GetConf.KAFKA_AV_TOPIC));

        try {
            ObjectMapper objectMapper = new ObjectMapper();

            // 在小数据规模下，不需要用set来去重，记录上一个记录就可以了
            String prevHost = "";

            // 拉取消息并消费
            for (int i = 0; i < 30; i++) {
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String,String> record:records){
                    JsonNode jsonNode = objectMapper.readTree(record.value());

                    // 查找有数据的部分
                    JsonNode urlNode = jsonNode.get("url");
                    if (jsonNode.get("dir").asInt() == 2 || urlNode == null)
                        continue;
                    String urlPath = URLHandleUtils.getFirstURLPath(urlNode.asText());
                    if (prevHost.equals(urlPath))
                        continue;
                    prevHost = urlPath;

                    // 交给查询模块进行处理
                    System.out.println(urlPath);
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    String urlPath = "test.com";

    @Test
    public void judgeKey() {
        JedisPoolUtils.getJedisPoolInstance();
        Jedis jedis = JedisPoolUtils.getJedis();
        if (jedis.exists(urlPath)) {
            // 存在的话再判断一下时间过期没有
            long writeTime = Long.parseLong(jedis.hget(urlPath, "write_time"));
            long nowTime = System.currentTimeMillis()/1000L;
            long earlyMorning3 = nowTime - (nowTime + 8 * 3600) % 86400 - 86400 * 2;
            if (earlyMorning3 > writeTime) {
                // 序列挖掘
                //sequenceMining(urlPath);
                System.out.println("需要进行序列挖掘");
            }
            else {
                System.out.println(String.format("不需要进行序列挖掘，上次的时间为%s", writeTime));
            }
        }
        JedisPoolUtils.releaseResource(jedis);
    }

    @Test
    public void isMore10() {
        AVDataListMapper avListMapper;
        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        avListMapper = sqlSession.getMapper(AVDataListMapper.class);
        String likeUrl = '%' + urlPath + '%';
        long effectiveTime = System.currentTimeMillis()/1000L - 3000000;
        int cnt = avListMapper.cumulativeData(likeUrl, effectiveTime);
        System.out.println(cnt);
    }

    @Test
    public void sequenceMining() {
    }

    @Test
    public void relateHostMiningTask() {
    }

    @Test
    public void buildCandidate() {
    }
}