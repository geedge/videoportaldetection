package cn.ac.iie.mining;

import cn.ac.iie.Utils.JedisPoolUtils;
import cn.ac.iie.Utils.SparkFactoryUtils;
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
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.collection.mutable.WrappedArray;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 代码逻辑：
 * 从kafka从读取数据，查看其host是否在redis中存在，如果存在则表示曾进行过序列挖掘，再检查这个hodt对应的记录是否失效，
 * 如果这个host在redis不存在或者已失效，则查ClickHouse看其历史数据是否大于10，如果大于则进行序列挖掘，并将结果写到redis中
 *
 * 当前问题：CK表的 AV_event 和 connection_record_log_local 中记录不一致，一个有的另一个可能没有，两个都有的时间可能对不上
 */
public class MiningService {
    public static final String groupId="test";
    public static final AtomicBoolean isRunning=new AtomicBoolean(true);
    private static Logger logger = LoggerFactory.getLogger(MiningService.class);

    private static Properties initConfig() {
        Properties properties=new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", GetConf.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // max.poll.records设置的条数要在一定的时间间隔处理完
        properties.put("max.poll.records", 10);
        // 自动提交
        properties.put("enable.auto.commit", "false");
        // 测试的时候设置为从头开始
        //properties.put("auto.offset.reset", "earliest");

        return properties;
    }

    private SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
    private static Jedis jedis;
    private AVDataListMapper avListMapper = sqlSession.getMapper(AVDataListMapper.class);;

    /**
     * TODO:各种session的范围应该放在方法里，保证可以合理关闭和私有
     * 这里在静态变量开启了
     */
    static {
        if (jedis == null) {
            JedisPoolUtils.getJedisPoolInstance();
            jedis = JedisPoolUtils.getJedis();
        }
    }

    public void consumeKafka() {
        Properties properties=initConfig();
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(GetConf.KAFKA_AV_TOPIC));

        try {
            ObjectMapper objectMapper = new ObjectMapper();

            // 在小数据规模下，不需要用set来去重，记录上一个记录就可以了
            String prevHost = "";

            // 拉取消息并消费
            finalTag:
            while(isRunning.get()){
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
                    logger.warn(String.format("当前YSP为%s", urlPath));
                    miningProcess(urlPath);
                    //break finalTag;
                }
                //consumer.commitSync();
                //break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            consumer.close();
            sqlSession.close();
            JedisPoolUtils.releaseResource(jedis);
        }
    }

    public List<String> mutiPrefixSpan(String yspHost, Boolean mutiTag) {
        List<List<TrafficLog>> sawContext = new ArrayList<>();
        int handleWay = inputMemChecker(yspHost, sawContext);
        if (handleWay != 1) {
            // 此时使用prefixSpan会造成OOM，该条舍去
            return null;
        }
        ArrayList<Row> data = buildInput(sawContext);
        logger.warn("数据构造完毕，开始序列挖掘");
        // 调用spark模块
        Dataset<Row> sequence = relateHostMiningTask(data);
        List<String> candidateList = miningCandidate(sequence);
        String allHisCandidate = getAllHisList(sawContext, yspHost);
        candidateList.add(0, allHisCandidate);
        if (!mutiTag) return candidateList;

        /*
         * muti部分
         * 这里存在两个性能不必要的损耗点：
         * 1.取最频繁一项的部分会带来性能的些微损失从candidate的String类型构建
         * 2.从最开始的sawContext构建，还需要重新构建spark需求的格式
         * 这里还有两个权宜之计：
         * 1.只用了频繁1序列去做对比
         * 2.最多再迭代一次
         * 之所以这样写是因为，方面，简洁，易懂，并且目前不需要时间复杂度的极限要求
         */
        List<List<TrafficLog>> restSawContext = new ArrayList<>();
        String rule1candi = candidateList.get(2);
        String maybePortal = rule1candi.substring(1, rule1candi.length()-1).split(", ")[0];
        for (List<TrafficLog> eachContext: sawContext) {
            boolean hasTag = false;
            for (TrafficLog eachLog: eachContext) {
                String oneItemHTTPHost = eachLog.getHttp_host();
                String oneItemSNI = eachLog.getSsl_sni();
                if (oneItemHTTPHost.equals(maybePortal) || oneItemSNI.equals(maybePortal)) {
                    hasTag = true;
                    break;
                }
            }
            if (!hasTag) {
                restSawContext.add(eachContext);
            }
        }

        if (restSawContext.size() < Configurations.getIntProperty(0, "leastHistoryNum")) {
            // 虽然mutiTag为true，但是无法进一步挖掘，退化为rule1
            logger.warn("不符合muti的条件，剩余上下文数量过少");
            candidateList.add(candidateList.get(2));
            return candidateList;
        }
        ArrayList<Row> data2 = buildInput(restSawContext);
        logger.warn("muti数据构造完毕，开始序列挖掘");
        // 调用spark模块
        Dataset<Row> sequence2 = relateHostMiningTask(data2);
        List<String> candidateList2 = miningCandidate(sequence2);
        // 拼接一下，特别要注意这里candidateList第2个是rule1，candidateList2第1个是rule1
        String oldCandi = candidateList.get(2);
        String leftPart = oldCandi.substring(0, oldCandi.length()-1);
        String rightPart = candidateList2.get(1).substring(1);
        String newCandi = leftPart.concat(", ").concat(rightPart);
        candidateList.add(newCandi);
        return candidateList;
    }

    public void miningProcess(String urlHost) {
        if (!judgeKeyCache(urlHost)) {
            logger.warn(String.format("%s cache不存在", urlHost));
        }

        List<String> candidateList = mutiPrefixSpan(urlHost, false);

        if (candidateList != null) {
            logger.warn("mining结束，写回Redis");
            jedis.hset(urlHost, "write_time", String.valueOf(System.currentTimeMillis()/1000L));
            Integer candidateSize = Configurations.getIntProperty(0, "candidateSize");
            if (candidateSize != null && candidateSize > 0) {
                String lastOptimize = candidateList.get(candidateList.size()-1);
                List<String> lastOptimizeList = Arrays.asList(lastOptimize.substring(1, lastOptimize.length()-1).split(", "));
                if (lastOptimizeList.size() > candidateSize) {
                    String candidate = lastOptimizeList.subList(0 ,candidateSize).toString();
                    jedis.hset(urlHost, "candidate", candidate);
                }
                else jedis.hset(urlHost, "candidate", candidateList.get(candidateList.size()-1));
            }
            else jedis.hset(urlHost, "candidate", candidateList.get(candidateList.size()-1));
        }
        else {
            logger.error(String.format("对%s进行挖掘容易造成堆内存溢出或出现频次太少，暂不处理", urlHost));
        }
    }

    /**
     * 查询redis中这条key是否在3天内已经进行过序列挖掘
     * 判断redis（入口表）中是否存在记录，是否过期
     * 注意这里如果在序列表中加了记录的过期删除的话需要修改代码哟～
     * @param urlPath 以 / 分割的一级地址
     */
    public boolean judgeKeyCache(String urlPath) {
        // TODO:下面这句貌似可以只写后面那个判断
        if (jedis.exists(urlPath) && jedis.hget(urlPath, "candidate") != null) {
            // 存在的话再判断一下时间过期没有
            long writeTime = Long.parseLong(jedis.hget(urlPath, "write_time"));
            long nowTime = System.currentTimeMillis()/1000L;
            // long earlyMorning3 = nowTime - (nowTime + 8 * 3600) % 86400 - 86400 * 2;
            long expireTime = nowTime - Configurations.getIntProperty(0, "ExpirationTime") * 24 * 60 * 60;
            if (expireTime > writeTime || jedis.hget(urlPath, "candidate").length() <= 2) {
                // 序列挖掘
                logger.warn(String.format("%s不存在且过去日志符合条件，进行序列挖掘", urlPath));
                return true;
            }
            else logger.warn(String.format("%s在Redis中存在并且尚未过期", urlPath));
        }
        else {
            // 序列挖掘
            if (isMore10(urlPath)) {
                logger.warn(String.format("%s不存在且过去日志符合条件，进行序列挖掘", urlPath));
                return true;
            }
            else {
                logger.error(String.format("%s在过去日志中出现次数过少", urlPath));
            }
        }
        return false;
    }

    /**
     * 查询ClickHouse看是否满足序列挖掘条件，单独抽出这个类的原因是避免用like直接去查询全流量泛收表
     * ⚠️：cumulativeData 是根据 like 去查询音视频流量表，而不是用整个 url 去查询，在大流量环境下可以替换为整个 url
     * @param urlPath 从 / 分割的一级地址
     * @return 是否可以进行序列挖掘
     */
    public boolean isMore10(String urlPath) {
        String likeUrl = '%' + urlPath + '%';
        // long effectiveTime = System.currentTimeMillis()/1000L - 3000000;
        long effectiveTime = System.currentTimeMillis()/1000L -
                Configurations.getIntProperty(0, "historyTrafficInterval") * 24 * 60 * 60;
        int cnt = avListMapper.cumulativeData(likeUrl, effectiveTime);
        return cnt >= Configurations.getIntProperty(0, "leastHistoryNum");
    }

    public ArrayList<Row> buildInput(List<List<TrafficLog>> sawContext) {
        ArrayList<Row> data = new ArrayList<>();

        for (List<TrafficLog> eachLogList: sawContext) {
            if (eachLogList.size() > Configurations.getIntProperty(0, "leastHistoryNum"))
                data.add(RowFactory.create(constructRow(eachLogList)));
        }
        return data;
    }

    /**
     * 先查ck的av表找到每一个相似的host的client ip和server ip，时间戳
     * 通过av标的每一条记录查询总流量表找到时间窗口
     * 每一个host找其时间窗口后，构造一行RowFactory
     * 将构造好的data交给Spark进行序列挖掘
     * 返回结果处理后写入redis
     *
     * 注意：该函数还会改变sawContext的值
     *
     * redis格式：
     * key: urlPath field: write_time, candidate 皆为字符型
     * candidate(String): [host1, host2, host3]
     */
    public int inputMemChecker(String urlHost, List<List<TrafficLog>> sawContext) {
        // 先查ck找到每一个相似的host
        String likeUrl = '%' + urlHost + '%';
        List<AVlog> aVlogs = avListMapper.getHistoryHost(likeUrl, Configurations.getIntProperty(0, "rawLimit"));
        logger.warn(String.format("%s过去曾出现过%d次", urlHost, aVlogs.size()));
        int historyCnt = aVlogs.size();
        int maxColSize = Math.min(historyCnt*Configurations.getIntProperty(0, "ratioRawCol"),
                Configurations.getIntProperty(0, "colLimit"));
        int overflowCols = 0;

        if (sawContext == null) {
            sawContext = new ArrayList<>();
        }
        for (AVlog eachLog: aVlogs) {
            List<TrafficLog> trafficLogList = avListMapper.getRelationListWithHost(eachLog.getFound_time(),
                    eachLog.getClient_ip(),
                    Configurations.getIntProperty(0, "intervalLeft"),
                    Configurations.getIntProperty(0, "intervalRight"));
            int trafficSize = trafficLogList.size();
            if (trafficSize > 0) {
                sawContext.add(trafficLogList);
                if (trafficSize > maxColSize)
                    overflowCols += 1;
            }
        }

        if (historyCnt - overflowCols < Configurations.getIntProperty(0, "leastHistoryNum"))
            return -1;
        else return 1;
    }

    /**
     * 工具方法：构造Spark序列挖掘的一行输入
     */
    private List<List<String>> constructRow(List<TrafficLog> trafficLogList) {
        List<List<String>> row = new ArrayList<>();
        for (TrafficLog trafficLog: trafficLogList) {
            String oneItemHTTPHost = trafficLog.getHttp_host();
            String oneItemSNI = trafficLog.getSsl_sni();
            // logger.warn("HTTP: " + oneItemHTTPHost);
            // logger.warn("SSL: " + oneItemSNI);
            if (oneItemSNI != null && oneItemSNI.length() > 0)
                row.add(Arrays.asList(oneItemSNI));
            else if (oneItemHTTPHost != null && oneItemHTTPHost.length() > 0)
                row.add(Arrays.asList(oneItemHTTPHost));
        }
        return row;
    }

    /**
     * 序列挖掘方法
     */
    public Dataset<Row> relateHostMiningTask(List<Row> data) {
        StructType schema = new StructType(new StructField[]{ new StructField(
                "sequence", new ArrayType(new ArrayType(DataTypes.StringType, true), true),
                false, Metadata.empty())
        });
        Dataset<Row> sequenceDF = SparkFactoryUtils.getSparkSessionFactory().createDataFrame(data, schema);
        // sequenceDF.show(20, true);
        PrefixSpan prefixSpan = new PrefixSpan()
                .setMinSupport(Configurations.getDoubleProperty(0, "prefixConfidence"))
                .setMaxPatternLength(Configurations.getIntProperty(0, "prefixMaxPatternLength"));

        // Finding frequent sequential patterns
        Dataset<Row> sequence = prefixSpan.findFrequentSequentialPatterns(sequenceDF);
        sequence.show(10, false);
        return sequence;
    }

    /**
     * Note：
     * 将spark序列挖掘返回的结果进行处理，返回候选集
     * 如果有需要重排序的也在这一步进行处理
     * 12.23更新：添加了多入口挖掘，目前匹配原数据这一块暂时首项而非首项的最长频繁序列
     *
     * 提醒自己的：
     * 总感觉Spark用Iterator的处理很慢，不如lambda
     * 这里项集的域名的地址没有经过处理
     * 这里有一定概率内存溢出，目前暂不清楚如何增加一个task的最大内存
     */
    public List<String> miningCandidate(Dataset<Row> sequence) {
        List<String> candidateList = new ArrayList<>();
        HashMap<String, Integer> restFirstMap = new HashMap<>();
        HashMap<String, Integer> restMap = new HashMap<>();

        String candidateItem;
        HashMap<String, Long> hashMap = new HashMap<>();
        // TODO:这里会导致堆内存不足的问题 Uncaught exception in thread task-result-getter
        for(Iterator<Row> iter = sequence.toLocalIterator(); iter.hasNext();) {
            Row each = iter.next();
            if (each.size() == 2) {
                try {
                    WrappedArray rawSequence = (WrappedArray)each.get(0);
                    WrappedArray firstItem = (WrappedArray)rawSequence.apply(0);
                    //if (cmt >= volume) break;
                    //cmt++;
                    if (rawSequence.size() == 1) {
                        candidateItem = (String)firstItem.apply(0);
                        Long freq = (Long) each.get(1);
                        if (hashMap.containsKey(candidateItem)) {
                            if (hashMap.get(candidateItem) < freq)
                                hashMap.put(candidateItem, freq);
                        }
                        else {
                            candidateList.add(candidateItem);
                            hashMap.put(candidateItem, freq);
                        }
                    }
                    else {
                        candidateItem =  (String)((WrappedArray)rawSequence.apply(0)).apply(0);
                        restFirstMap.put(candidateItem, restFirstMap.getOrDefault(candidateItem, 0) + 1);
                        for (int i = 1; i < rawSequence.size(); i++) {
                            candidateItem = (String)((WrappedArray)rawSequence.apply(i)).apply(0);
                            restMap.put(candidateItem, restMap.getOrDefault(candidateItem, 0) + 1);
                        }
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else break;
        }
        candidateList.sort((o1, o2) -> (int) (hashMap.get(o2) - hashMap.get(o1)));
        List<String> optimizedOrders = new ArrayList<>();
        // 根据freq排，但是感觉freq并不是统计的结果呀，就很奇怪
        optimizedOrders.add(candidateList.toString());
        logger.warn("计算结果：" + candidateList.toString());

        prefixReSort(optimizedOrders, candidateList, restFirstMap, restMap);
        logger.warn("优化后的结果：" + optimizedOrders.get(optimizedOrders.size()-1));
        return optimizedOrders;
    }

    /**
     * 直接用一项集的话，结果和统计是没差的，其实入口更倾向于出现在多项集的首部，而不是一项集
     * 规则1：入口更倾向于出现在多项集的首部 -> 统计candidate出现在多项集首部的次数
     * 规则2：入口更不倾向于出现在无序集中 -> 统计candidate出现在不是首部的比例（这一点目前来看不一定是好的，因为很多大站很可能就在后面会出现呀～）
     *
     * @param candidateList 一项集
     * @param restMap 多项集
     * 几种不同的优化后的候选集顺序[统计， 规则1，统计+规则1，规则2，统计+规则1+规则2]
     *                还有一个总体统计的，但是比较占内存，这里没有用啦～
     *                忽然觉得用prefix items为多的结果，是否有点不太好，因为3项集多的2项集也会多
     */
    public void prefixReSort(List<String> orders, List<String> candidateList, Map<String, Integer> restFirstMap, Map<String, Integer> restMap) {
        int candidateListSize = candidateList.size();
        HashMap<String, Integer> ruleValueMap = new HashMap<>();
        for (int i = 0; i < candidateListSize; i++) {
            ruleValueMap.put(candidateList.get(i), i);
        }

        // default设置为0，使在首部出现的往前面移动，其中restFirstMap是在首部出现的次数
        candidateList.sort((o1, o2) -> (restFirstMap.getOrDefault(o2, 0) - restFirstMap.getOrDefault(o1, 0)));
        resort(candidateList, orders, candidateListSize, ruleValueMap);

        // 尤其是下面这里，不是很合适
        // default设置为3，使经常在后面出现的排序往后移动，其中restMap是在非首部出现的次数
        candidateList.sort(Comparator.comparingInt(o -> restMap.getOrDefault(o, 3)));
        resort(candidateList, orders, candidateListSize, ruleValueMap);
    }

    /**
     * 统计ysp总体上下文
     * @param sawContext 从历史数据库中查到的上下文List
     * @param yspHost ysp的host，打日志用
     * @return 统计的候选集 candidateSize 限制了大小，如果不限制大小会浪费很大的内存
     */
    public String getAllHisList(List<List<TrafficLog>> sawContext, String yspHost) {
        logger.warn(String.format("对%s进行总量统计", yspHost));
        HashMap<String, Integer> cntAllMap = new HashMap<>();
        for (List<TrafficLog> eachList: sawContext) {
            for (TrafficLog trafficLog: eachList) {
                String oneItemHTTPHost = trafficLog.getHttp_host();
                String oneItemSNI = trafficLog.getSsl_sni();
                if (oneItemHTTPHost != null && oneItemHTTPHost.length() > 0)
                    cntAllMap.put(oneItemHTTPHost, cntAllMap.getOrDefault(oneItemHTTPHost, 0) + 1);
                else if (oneItemSNI != null && oneItemSNI.length() > 0)
                    cntAllMap.put(oneItemSNI, cntAllMap.getOrDefault(oneItemSNI, 0) + 1);
            }
        }
        List<String> hisAllList = new ArrayList<>(cntAllMap.keySet());
        hisAllList.sort( (o1, o2) -> cntAllMap.get(o2) - cntAllMap.get(o1) );
        int candidateSize = Configurations.getIntProperty(0, "candidateSize");
        if (hisAllList.size() > candidateSize)
            return hisAllList.subList(0, candidateSize).toString();
        else return hisAllList.toString();
    }

    private void resort(List<String> candidateList, List<String> orders, int candidateListSize, HashMap<String, Integer> ruleValueMap) {
        orders.add(candidateList.toString());
        for (int i = 0; i < candidateListSize; i++) {
            String item = candidateList.get(i);
            ruleValueMap.put(item, ruleValueMap.get(item) + i);
        }
        candidateList.sort((o1, o2) -> (ruleValueMap.get(o1) - ruleValueMap.get(o2)));
        orders.add(candidateList.toString());
    }
}
