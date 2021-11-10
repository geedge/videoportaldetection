package cn.ac.iie.intervalStatics;

import cn.ac.iie.Utils.JedisPoolUtils;
import cn.ac.iie.Utils.SparkFactoryUtils;
import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.Utils.URLHandleUtils;
import cn.ac.iie.config.Configurations;
import cn.ac.iie.mapper.AVDataListMapper;
import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.ml.fpm.PrefixSpan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.collection.mutable.WrappedArray;

import java.io.*;
import java.util.*;

public class FeatureGenerator {
    static {
        InputStream inputStream=FeatureGenerator.class.getResourceAsStream("/log4j.properties");
        PropertyConfigurator.configure(inputStream);
    }
    private static Logger logger = LoggerFactory.getLogger(FeatureGenerator.class);

    /**
     * 提取特征向量：(总体统计的个数，prefix总体的个数，prefix子序列第一位出现的次数，prefix子序列在后面出现的次数，编辑距离)
     * 待补充：当前host与ysp host的编辑距离
     * 注意：需要删除200.200.200.200；如果部分host在prefix的子序列不出现的话就把这几个值设置为0
     * 待测试的方案：分类，learning 2 rank
     * 评估方式：Index avg 和 Prep pos=1
     * 输出结果：每一个ysp host的向量彼此间隔，其下每一行的格式为[host, true/false, 特征向量]
     */
    public void featureExtracFromFile() {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream("testSet.txt");
             Writer writer = new OutputStreamWriter(new FileOutputStream("feature.txt"), "UTF-8");
             SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession()) {
            if (is == null) {
                System.out.println("文件不存在");
                System.exit(0);
            }
            AVDataListMapper avListMapper = sqlSession.getMapper(AVDataListMapper.class);

            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            Jedis jedis = JedisPoolUtils.getJedis();

            String line = "";
            while((line = br.readLine())!=null) {
                String[] item = line.split(" ");
                String yspHost = URLHandleUtils.getFirstURLPath(item[1]);
                String refererHost = item[2];

                // 先计算每一个候选集，然后判断当前host在哪个位置
                // 用特征结果来表示redis的key，结尾加一个FeatExtrac后缀
                if (jedis != null && jedis.exists(yspHost) && !yspHost.startsWith("200.200.200.200")) {
                    // 因为之前写入的redis的都是符合条件的，所以这里可以用缓存来判断
                    // 如果单独运行的话，这里其实需要进行一次
                    writer.write(yspHost + " " + refererHost + "\r\n");

                    String candSet = jedis.hget(yspHost, "candSet");
                    if (candSet == null) {
                        recallAndResort(yspHost, refererHost, writer, jedis, avListMapper);
                    }
                    else {
                        logger.warn(String.format("%s在Cache中存在", yspHost));
                        String[] candSetArray = candSet.substring(1, candSet.length() - 1).split(", ");
                        for (String eachHost: candSetArray) {
                            String hostKey = eachHost + "_FeatExtrac";
                            String cacheLine = jedis.hget(yspHost, hostKey);
                            if (cacheLine == null) {
                                logger.error("candSetArray而cache不存在，这里应该有问题！");
                                continue;
                            }
                            // 注意cacheLine的格式
                            writer.write(eachHost + "," + eachHost.equals(refererHost) + "," + cacheLine);
                        }
                    }
                }
                else continue;

                writer.write("------------------------------------------------------\r\n");
                //break;
            }
            JedisPoolUtils.releaseResource(jedis);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 把每一行写入训练数据中。这里为了防止host特别多，设置写入的训练数据必须在两个候选集中出现。
     * 如果refer没有在两个候选集中出现，那么返回0，否则返回1
     * @param writer
     * @param rule1Map 一项集的Map，value为次数
     * @param rule2Map 规则2的Map，value也是在后面（非首部）出现了多少次
     * @param allHisMap 全统计的Map，value是出现次数，注意allHisMap可能会非常大
     * @param candSet 当然是候选集啦～
     */
    private int featureWrite(String urlHost, String referHost, Writer writer, Jedis jedis,
                             Map<String, Integer> allHisMap, Map<String, Long> prefixCntMap,
                             Map<String, Integer> rule1Map, Map<String, Integer> rule2Map, Set<String> candSet) throws IOException {
        for (String thisHost : candSet) {
            if (rule1Map.containsKey(thisHost) || allHisMap.containsKey(thisHost)) {
                // 注意这里默认为0，但是就语义而言，rule2的为0是最好的，不过暂时还是设置为0吧
                String value = allHisMap.getOrDefault(thisHost, 0) + "," +
                        prefixCntMap.getOrDefault(thisHost, (long) 0) + "," +
                        rule1Map.getOrDefault(thisHost, 0) + "," +
                        rule2Map.getOrDefault(thisHost, 0) + "," +
                        minDistance(urlHost, thisHost) + "\r\n";
                writer.write(thisHost + "," + thisHost.equals(referHost) + "," + value);
                String hostKey = thisHost + "_FeatExtrac";
                jedis.hset(urlHost, hostKey, value);
            }
        }

        if (rule1Map.containsKey(referHost) || allHisMap.containsKey(referHost))
            return 1;
        else return 0;
    }

    public int minDistance(String word1, String word2) {
        int word1Len = word1.length();
        int word2Len = word2.length();
        if (word1Len == 0 || word2Len == 0) {
            return Math.max(word1Len, word2Len);
        }

        int[][] dp = new int[word1Len+1][word2Len+1];

        //初始化两条边界
        for (int i = 0; i <= word1Len; i++) {
            dp[i][0] = i;
        }
        for (int j = 1; j <= word2Len; j++) {
            dp[0][j] = j;
        }

        //循环更新
        for (int i = 1; i <= word1Len; i++) {
            for (int j = 1; j <= word2Len; j++) {
                if (word1.charAt(i-1) == word2.charAt(j-1)) {
                    dp[i][j] = Math.min(Math.min(dp[i-1][j] + 1, dp[i][j-1] + 1), dp[i-1][j-1]);
                }
                else dp[i][j] = Math.min(Math.min(dp[i-1][j] + 1, dp[i][j-1] + 1), dp[i-1][j-1] + 1);
            }
        }
        return dp[word1Len][word2Len];
    }

    private void recallAndResort(String yspHost, String referHost, Writer writer, Jedis jedis, AVDataListMapper avListMapper) throws IOException {
        String likeUrl = '%' + yspHost + '%';
        List<AVlog> aVlogs = avListMapper.getHistoryHost(likeUrl, Configurations.getIntProperty(0, "rawLimit"));
        logger.warn(String.format("%s过去曾出现过%d次", yspHost, aVlogs.size()));

        // 统计历史总量的Hash表，用于召回
        HashMap<String, Integer> allHisMap = new HashMap<>();
        // 编辑距离的Hash表，用于召回
        HashMap<String, Integer> distanceHash = new HashMap<>();

        ArrayList<Row> data = new ArrayList<>();
        int historyCnt = aVlogs.size();
        int maxColSize = Math.min(historyCnt * Configurations.getIntProperty(0, "ratioRawCol"),
                Configurations.getIntProperty(0, "colLimit"));
        int overflowCols = 0;
        for (AVlog eachLog: aVlogs) {
            List<TrafficLog> trafficLogList = avListMapper.getRelationListWithHost(eachLog.getFound_time(),
                    eachLog.getClient_ip(),
                    Configurations.getIntProperty(0, "intervalLeft"),
                    Configurations.getIntProperty(0, "intervalRight"));
            int trafficSize = trafficLogList.size();
            if (trafficSize > 0) {
                if (trafficSize > maxColSize)
                    overflowCols += 1;
                else {
                    List<List<String>> row = new ArrayList<>();
                    for (TrafficLog trafficLog: trafficLogList) {
                        String oneItemHTTPHost = trafficLog.getHttp_host();
                        String oneItemSNI = trafficLog.getSsl_sni();
                        // logger.warn("HTTP: " + oneItemHTTPHost);
                        // logger.warn("SSL: " + oneItemSNI);
                        if (oneItemSNI != null && oneItemSNI.length() > 0) {
                            allHisMap.put(oneItemSNI, allHisMap.getOrDefault(oneItemSNI, 0) + 1);
                            row.add(Arrays.asList(oneItemSNI));
                            if (!distanceHash.containsKey(oneItemSNI)) {
                                distanceHash.put(oneItemSNI, minDistance(yspHost, oneItemSNI));
                            }
                        }
                        else if (oneItemHTTPHost != null && oneItemHTTPHost.length() > 0) {
                            allHisMap.put(oneItemHTTPHost, allHisMap.getOrDefault(oneItemHTTPHost, 0) + 1);
                            row.add(Arrays.asList(oneItemHTTPHost));
                            if (!distanceHash.containsKey(oneItemHTTPHost)) {
                                distanceHash.put(oneItemHTTPHost, minDistance(yspHost, oneItemHTTPHost));
                            }
                        }
                    }
                    data.add(RowFactory.create(row));
                }
            }
        }

        if (historyCnt - overflowCols < Configurations.getIntProperty(0, "leastHistoryNum")) {
            logger.warn("不满足mining条件");
            return;
        }

        Set<String> candSet = recallCandidate(allHisMap, distanceHash);

        logger.warn("数据构造完毕，开始序列挖掘");
        StructType schema = new StructType(new StructField[]{ new StructField(
                "sequence", new ArrayType(new ArrayType(DataTypes.StringType, true), true),
                false, Metadata.empty())
        });
        Dataset<Row> sequenceDF = SparkFactoryUtils.getSparkSessionFactory().createDataFrame(data, schema);
        sequenceDF.show(20, true);
        //sequenceDF.show(50, false);
        PrefixSpan prefixSpan = new PrefixSpan().setMinSupport(Configurations.getDoubleProperty(0, "prefixConfidence")).setMaxPatternLength(5);
        // Finding frequent sequential patterns
        Dataset<Row> sequence = prefixSpan.findFrequentSequentialPatterns(sequenceDF);
        sequence.show(10, false);

        // 这里重写build candidate
        HashMap<String, Integer> restFirstMap = new HashMap<>();
        HashMap<String, Integer> restMap = new HashMap<>();

        String candidateItem;
        HashMap<String, Long> prefixCntMap = new HashMap<>();
        for(Iterator<Row> iter = sequence.toLocalIterator(); iter.hasNext();) {
            Row each = iter.next();
            if (each.size() == 2) {
                try {
                    WrappedArray rawSequence = (WrappedArray)each.get(0);
                    WrappedArray firstItem = (WrappedArray)rawSequence.apply(0);
                    if (rawSequence.size() == 1) {
                        // 一项集
                        candidateItem = (String)firstItem.apply(0);
                        Long freq = (Long) each.get(1);
                        prefixCntMap.put(candidateItem, freq);
                        //if (prefixCntMap.containsKey(candidateItem)) {
                        //    if (prefixCntMap.get(candidateItem) < freq)
                        //        prefixCntMap.put(candidateItem, freq);
                        //}
                        //else {
                        //    candSet.add(candidateItem);
                        //    prefixCntMap.put(candidateItem, freq);
                        //}
                    }
                    else {
                        // 非一项集
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

        jedis.hset(yspHost, "candSet", candSet.toString());
        // 接下来调用featureWrite并写到redis里面，featureWrite的0或者1不需要写入，因为redis中不会记录
        featureWrite(yspHost, referHost, writer, jedis, allHisMap, prefixCntMap, restFirstMap, restMap, candSet);
    }

    private Set<String> recallCandidate(HashMap<String, Integer> allHisMap, HashMap<String, Integer> distanceHash) {
        // 这里的candidateSet由统计和prefix一项集组成
        List<String> hisAllList = new ArrayList<>(allHisMap.keySet());
        hisAllList.sort( (o1, o2) -> allHisMap.get(o2) - allHisMap.get(o1) );
        int candidateSize = Configurations.getIntProperty(0, "candidateSize");
        List<String> candidateList;
        if (hisAllList.size() > candidateSize)
            candidateList = hisAllList.subList(0, candidateSize);
        else candidateList = hisAllList;
        Set<String> candSet = new HashSet<>(candidateList);

        int len = candSet.size();
        hisAllList.sort(Comparator.comparingInt(distanceHash::get));
        for (int i = 0; i < len; i++) {
            candSet.add(hisAllList.get(i));
        }

        return candSet;
    }

    public void refreshCache() {
        logger.warn("清除Cache");
        Jedis jedis = JedisPoolUtils.getJedis();
        assert jedis != null;
        Set<String> cacheKeys = jedis.keys("*");
        for (String key: cacheKeys) {
            jedis.hdel(key, "candSet");
        }
        JedisPoolUtils.returnResource(jedis);
    }


    public static void main(String[] args) {
        FeatureGenerator featureGenerator = new FeatureGenerator();
        logger.warn("特征生成任务开始...");
        featureGenerator.refreshCache();
        featureGenerator.featureExtracFromFile();
    }

}