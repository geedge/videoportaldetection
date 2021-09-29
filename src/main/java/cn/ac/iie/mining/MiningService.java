package cn.ac.iie.mining;

import cn.ac.iie.Utils.JedisPoolUtils;
import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.Utils.URLHandleUtils;
import cn.ac.iie.config.Configurations;
import cn.ac.iie.config.GetConf;
import cn.ac.iie.mapper.AVDataListMapper;
import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ibatis.session.SqlSession;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.ml.fpm.PrefixSpan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 代码逻辑：
 * 从kafka从读取数据，查看其host是否在redis中存在，如果存在则表示曾进行过序列挖掘，再检查这个hodt对应的记录是否失效，
 * 如果这个host在redis不存在或者已失效，则查ClickHouse看其历史数据是否大于10，如果大于则进行序列挖掘，并将结果写到redis中
 */
public class MiningService {
    public static final String groupId="test";
    public static final AtomicBoolean isRunning=new AtomicBoolean(true);

    private static Properties initConfig() {
        Properties properties=new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", GetConf.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // max.poll.records设置的条数要在一定的时间间隔处理完
        properties.put("max.poll.records", 50);
        // 自动提交
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");

        return properties;
    }

    private static SqlSession sqlSession;
    private static Jedis jedis;
    private static AVDataListMapper avListMapper;

    static {
        sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        avListMapper = sqlSession.getMapper(AVDataListMapper.class);
        if (jedis == null) {
            JedisPoolUtils.getJedisPoolInstance();
            jedis = JedisPoolUtils.getJedis();
        }
    }

    private static SparkSession spark = SparkSession
            .builder()
            .appName("PrefixSpanHosts")
            .master(Configurations.getStringProperty(0, "Spark.master"))
            .getOrCreate();

    @Test
    public void consumeKafka() {
        Properties properties=initConfig();
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(GetConf.KAFKA_AV_TOPIC));

        try {
            ObjectMapper objectMapper = new ObjectMapper();

            // 在小数据规模下，不需要用set来去重，记录上一个记录就可以了
            String prevHost = "";

            // 拉取消息并消费
            while(isRunning.get()){
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String,String> record:records){
                    JsonNode jsonNode = objectMapper.readTree(record.value());

                    // 查找有数据的部分
                    if (jsonNode.get("dir").asInt() == 2)
                        continue;
                    String urlPath = URLHandleUtils.getFirstURLPath(jsonNode.get("url").asText());
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
            sqlSession.close();
            JedisPoolUtils.returnResource(jedis);
        }
    }

    /**
     * 查询redis中这条key是否在3天内已经进行过序列挖掘
     * 判断redis（入口表）中是否存在记录，是否过期
     * 注意这里如果在序列表中加了记录的过期删除的话需要修改代码哟～
     * @param urlPath 以 / 分割的一级地址
     */
    public void judgeKey(String urlPath) {
        if (jedis.exists(urlPath)) {
            // 存在的话再判断一下时间过期没有
            long writeTime = Long.parseLong(jedis.hget(urlPath, "write_time"));
            long nowTime = System.currentTimeMillis()/1000L;
            long earlyMorning3 = nowTime - (nowTime + 8 * 3600) % 86400 - 86400 * 2;
            if (earlyMorning3 > writeTime && isMore10(urlPath)) {
                // 序列挖掘
                sequenceMining(urlPath);
            }
        }
        else {
            // 序列挖掘
            if (isMore10(urlPath)) {
                sequenceMining(urlPath);
            }
        }
    }

    /**
     * 查询ClickHouse看是否满足序列挖掘条件
     * @param urlPath 从 / 分割的一级地址
     * @return 是否可以进行序列挖掘
     */
    public boolean isMore10(String urlPath) {
        String likeUrl = '%' + urlPath + '%';
        // 这个时间不是准确的30天前喔
        long effectiveTime = System.currentTimeMillis()/1000L - 3000000;
        int cnt = avListMapper.cumulativeData(likeUrl, effectiveTime);
        return cnt >= 10;
    }

    /**
     * 先查ck找到每一个相似的host的client ip和server ip，时间戳
     * 每一个host找其时间窗口，构造一行RowFactory
     * 将构造好的data交给Spark进行序列挖掘
     * 返回结果处理后写入redis
     *
     * redis格式：
     * key: urlPath field: write_time, candidate 皆为字符型
     * candidate(String): [host1, host2, host3]
     */
    public void sequenceMining(String urlPath) {
        // 先查ck找到每一个相似的host
        String likeUrl = '%' + urlPath + '%';
        List<AVlog> aVlogs = avListMapper.getHistoryHost(likeUrl);

        ArrayList<Row> data = new ArrayList<>();

        // 对于每一个host查找其对应窗口内的所有host
        for (AVlog eachLog: aVlogs) {
            List<TrafficLog> trafficLogList = avListMapper.getRelationListWithHost(eachLog.getFound_time(),
                    eachLog.getClient_ip(),
                    Configurations.getIntProperty(0, "interval"));
            data.add(RowFactory.create(constructRow(trafficLogList)));
        }

        // 调用spark模块
        Dataset<Row> sequence = relateHostMiningTask(spark, data);

        // 写回redis
        List<String> candidateList = buildCandidate(sequence);
        jedis.hset(urlPath, "write_time", String.valueOf(System.currentTimeMillis()/1000L));
        long returnStatus = jedis.hset(urlPath, "candidate", candidateList.toString());
    }

    /**
     * 工具方法：构造Spark序列挖掘的一行输入
     */
    private List<List<String>> constructRow(List<TrafficLog> trafficLogList) {
        List<List<String>> row = new ArrayList<>();
        for (TrafficLog trafficLog: trafficLogList) {
            row.add(Arrays.asList(trafficLog.getHttp_host()));
        }
        return row;
    }

    /**
     * 序列挖掘方法
     */
    public Dataset<Row> relateHostMiningTask(SparkSession spark, List<Row> data) {
        StructType schema = new StructType(new StructField[]{ new StructField(
                "sequence", new ArrayType(new ArrayType(DataTypes.StringType, true), true),
                false, Metadata.empty())
        });
        Dataset<Row> sequenceDF = spark.createDataFrame(data, schema);

        PrefixSpan prefixSpan = new PrefixSpan().setMinSupport(0.5).setMaxPatternLength(5);

        // Finding frequent sequential patterns
        Dataset<Row> sequence = prefixSpan.findFrequentSequentialPatterns(sequenceDF);
        sequence.show();
        return sequence;
    }

    /**
     * 将spark序列挖掘返回的结果进行处理，返回候选集
     * 如果有需要重排序的也在这一步进行处理
     * 总感觉Spark用Iterator的处理很慢，不如lambda
     */
    public List<String> buildCandidate(Dataset<Row> sequence) {
        List<String> oneItemList = new ArrayList<>();
        for(Iterator<Row> iter = sequence.toLocalIterator(); iter.hasNext();) {
            Row each = iter.next();
            if (each.size() == 1)
                oneItemList.add(each.getString(0));
            else break;
        }
        return oneItemList;
    }
}
