package cn.ac.iie.mining;


import cn.ac.iie.Utils.JedisPoolUtils;
import cn.ac.iie.Utils.URLHandleUtils;
import cn.ac.iie.config.Configurations;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class EffectEvaluation {
    private static Logger logger = LoggerFactory.getLogger(EffectEvaluation.class);

    /**
     * 评估指标：平均出现出现index最小的顺序，最小顺序中在第1次出现的占比，在前5次出现的占比，在前10次出现的占比，不出现的占比
     * 计算：出现次数，不出现的次数，每个方案出现的index之和，每个方案第1次出现的次数，每个方案在前5次出现的次数，每个方案在前10次出现的次数
     */
    public void evaluateFromFile(String evaFile) {
        String sourcePath = evaFile.startsWith("testSet") ? evaFile : "textSet.txt";
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(sourcePath)){
            if (is == null) {
                System.out.println("评估文件不存在");
                System.exit(0);
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            MiningService miningService = new MiningService();

            // prefix序列找到的数目
            int validateYspCnt = 0;
            // 统计找到的数目
            int validateStatisticsYspCnt = 0;
            // prefix序列的总长
            int validateYspSum = 0;
            // 当前处理的数目
            int yspCnt = 0;

            Map<String, Integer[]> evaluateMap = new HashMap<>();
            List<String> lostYspHost = new ArrayList<>();
            List<String> cantFindList = new ArrayList<>();
            int cnt200 = 0;

            // client_ip, url, 经过处理的referer, fount_time, refer的start_time, interval
            String line = "";
            while((line = br.readLine())!=null) {
                String[] item = line.split(" ");
                String cIp = item[0];
                String yspHost = URLHandleUtils.getFirstURLPath(item[1]);
                String refererHost = item[2];
                // 这里直接过滤掉200和haokan视频
                if (yspHost.startsWith("200.200.200.200") || refererHost.equals("haokan.baidu.com"))
                    continue;
                int[] resultIndex = itemEvaluate(yspHost, refererHost, miningService);

                yspCnt += 1;
                switch (resultIndex[0]) {
                    // 正常进行序列挖掘
                    case 1:
                        validateYspCnt += 1;
                        validateYspSum += resultIndex[resultIndex.length - 1];
                        accumulateResult(evaluateMap, "statistic", resultIndex[1]);
                        accumulateResult(evaluateMap, "rule1", resultIndex[2]);
                        accumulateResult(evaluateMap, "sta_rule1", resultIndex[3]);
                        accumulateResult(evaluateMap, "rule2", resultIndex[4]);
                        accumulateResult(evaluateMap, "sta_rule1_rule2", resultIndex[5]);
                        accumulateResult(evaluateMap, "statistics_all", resultIndex[6]);
                        if (resultIndex[6] == -1) cantFindList.add(yspHost);
                        if (yspHost.startsWith("200.200.200.200")) cnt200 += 1;
                        break;
                    // 防止OOM，仅采用统计
                    case 0:
                        // validateStatisticsYspCnt += 1;
                        // accumulateResult(evaluateMap, "statistics_all", resultIndex[6]);
                        // break;
                    // 数量过少，不符合众包思想，不进行统计或者序列挖掘
                    case -1:
                        lostYspHost.add(yspHost);
                        break;
                }

                if (yspCnt % 5 == 0) {
                    logger.warn(String.format("目前处理第%d条", yspCnt));
                }
            }
            printEvaluate(yspCnt, validateYspCnt, validateStatisticsYspCnt, validateYspSum, evaluateMap);
            printRecall(yspCnt, validateYspCnt, validateStatisticsYspCnt, validateYspSum, evaluateMap);
            //System.out.println(lostYspHost.toString());
            System.out.println(String.format("不符合众包条件（小于最小挖掘数目）的个数为：%d", lostYspHost.size()));
            System.out.println(String.format("防止OOM的个数位:%d", validateStatisticsYspCnt));
            System.out.println(String.format("众包未找到的个数为：%d", cantFindList.size()));
            //System.out.println(cantFindList.toString());
            System.out.println(String.format("其中200的占了多少：%d", cnt200));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 评估三种方式：
     * 统计的顺序
     * prefixSpan的一项集顺序
     * prefixSpan重排序后的顺序
     *
     * 如果要用redis去避免重复计算的话，需要同时存几种不同的方案
     * @return 返回值为长度为4的数组，[是否加入训练(0/1)，统计，规则1，统计+规则1，规则2，统计+规则1+规则2]
     */
    public int[] itemEvaluate(String yspHost, String refererHost, MiningService miningService) {
        Jedis jedis = JedisPoolUtils.getJedis();
        if (jedis != null && jedis.exists(yspHost)) {

            String statisticCandidate = jedis.hget(yspHost, "statistic");
            String rule1Candidate = jedis.hget(yspHost, "rule1");
            String staRule1Candidate = jedis.hget(yspHost, "sta_rule1");
            String rule2Candidate = jedis.hget(yspHost, "rule2");
            String prefixCandidate = jedis.hget(yspHost, "sta_rule1_rule2");
            String cntAllCandidate = jedis.hget(yspHost, "statistics_all");
            if (prefixCandidate == null || statisticCandidate == null ||
                    rule1Candidate == null || staRule1Candidate == null ||
                    rule2Candidate == null || cntAllCandidate == null) {
                // 此时计算
                logger.warn("redis缓存不全");
                int[] resultIndex = mining2Evaluate(yspHost, refererHost, miningService, jedis);
                JedisPoolUtils.returnResource(jedis);
                return resultIndex;
            }
            else {
                JedisPoolUtils.returnResource(jedis);
                logger.warn(String.format("%s在redis缓存中存在，无需重复计算", yspHost));
                // 此时从redisCandidate中判断
                return new int[]{
                        1,
                        judgeIndex(statisticCandidate, refererHost),
                        judgeIndex(rule1Candidate, refererHost),
                        judgeIndex(staRule1Candidate, refererHost),
                        judgeIndex(rule2Candidate, refererHost),
                        judgeIndex(prefixCandidate, refererHost),
                        judgeIndex(cntAllCandidate, refererHost),
                        statisticCandidate.split(", ").length
                };
            }
        }
        else {
            if (!miningService.isMore10(yspHost)) {
                logger.warn(String.format("%s数据过少，不需要进行挖掘", yspHost));
                return new int[]{-1, -1, -1, -1, -1, -1, -1, -1};
            }
            int[] resultIndex = mining2Evaluate(yspHost, refererHost, miningService, jedis);
            JedisPoolUtils.returnResource(jedis);
            return resultIndex;
        }
    }

    /**
     * @param yspHost 音视频
     * @param refererHost 入口
     * @return 按照规则得到的序列
     */
    private int[] mining2Evaluate(String yspHost, String refererHost, MiningService miningService, Jedis jedis) {
        List<List<TrafficLog>> sawContext = new ArrayList<>();
        int handleWay = miningService.inputMemChecker(yspHost, sawContext);
        String allHisCandidate = miningService.getAllHisList(sawContext, yspHost);
        if (handleWay == 1) {
            ArrayList<Row> data = miningService.buildInput(sawContext);
            logger.warn("数据构造完毕，开始序列挖掘");
            Dataset<Row> sequence = miningService.relateHostMiningTask(data);
            logger.warn("mining结束，写回Redis");
            List<String> candidateList = miningService.miningCandidate(sequence);
            if (jedis != null) {
                jedis.hset(yspHost, "write_time", String.valueOf(System.currentTimeMillis()/1000L));
                jedis.hset(yspHost, "statistic", candidateList.get(0));
                jedis.hset(yspHost, "rule1", candidateList.get(1));
                jedis.hset(yspHost, "sta_rule1", candidateList.get(2));
                jedis.hset(yspHost, "rule2", candidateList.get(3));
                jedis.hset(yspHost, "sta_rule1_rule2", candidateList.get(4));
                jedis.hset(yspHost, "statistics_all", allHisCandidate);
            }
            return new int[]{
                    1,
                    judgeIndex(candidateList.get(0), refererHost),
                    judgeIndex(candidateList.get(1), refererHost),
                    judgeIndex(candidateList.get(2), refererHost),
                    judgeIndex(candidateList.get(3), refererHost),
                    judgeIndex(candidateList.get(4), refererHost),
                    judgeIndex(allHisCandidate, refererHost),
                    candidateList.get(0).split(", ").length
            };
        }
        else {
            logger.error(String.format("对%s进行挖掘容易造成堆内存溢出，暂不处理", yspHost));
            return new int[]{0, -1, -1, -1, -1, -1, judgeIndex(allHisCandidate, refererHost), -1};
        }
    }

    /**
     * TODO:这里判断容易出问题，需要验证一下
     */
    private int judgeIndex(String candidate, String refererHost) {
        String[] candidateArray = candidate.substring(1, candidate.length()-1).split(", ");
        // 对入口大小进行限制
        int candidate_size = Configurations.getIntProperty(0, "candidateSize");
        for (int i = 0; i < candidateArray.length && i < candidate_size; i++) {
            if (refererHost.equals(candidateArray[i]))
                return i + 1;
        }
        return -1;
    }

    /**
     * 数组为：[出现次数，不出现的次数，每个方案出现的index之和，每个方案第1次出现的次数，每个方案在前5次出现的次数，每个方案在前10次出现的次数]
     */
    private void accumulateResult(Map<String, Integer[]> evaluateMap, String key, Integer idx) {
        if (!evaluateMap.containsKey(key)) {
            evaluateMap.put(key, new Integer[]{0,0,0,0,0,0});
        }
        Integer[] accResultIdx = evaluateMap.get(key);
        if (idx == -1) {
            // 因为是引用，所以直接改变内部的值就可以，不需要再put回去
            accResultIdx[1] += 1;
            return;
        }
        accResultIdx[0] += 1;
        accResultIdx[2] += idx;
        if (idx == 1) {
            accResultIdx[3] += 1;
            accResultIdx[4] += 1;
            accResultIdx[5] += 1;
        }
        else if (idx >= 2 && idx <= 5) {
            accResultIdx[4] += 1;
            accResultIdx[5] += 1;
        }
        else if (idx > 5 && idx <= 10) {
            accResultIdx[5] += 1;
        }
    }

    private void printEvaluate(int yspCmt, int validateYspCmt, int validateStatisticsYspCnt,
                               int validateYspSum, Map<String, Integer[]> evaluateMap) {
        System.out.println(String.format("在一定时间窗口且存在referer的YSP总数为%s，其中符合挖掘条件的个数为%s，prefix候选集平均长度为%s",
                yspCmt, validateYspCmt, validateYspSum /  (float) validateYspCmt));
        System.out.format("%18s%18s%18s%18s%18s%18s%18s%n", "Sorting rules", "Occurrences", "No-occurrence", "Index avg", "Prop pos=1", "Prop pos<5", "Prop pos<10");
        for (Map.Entry<String, Integer[]> entry : evaluateMap.entrySet()) {
            String key = entry.getKey();
            Integer[] value = entry.getValue();
            if (key.equals("statistics_all")) {
                System.out.format("%18s%18s%18s%18.3f%18.3f%18.3f%18.3f%n",
                        key, value[0], value[1],
                        value[2] / (float) (validateYspCmt + validateStatisticsYspCnt),
                        value[3] / (float) (validateYspCmt + validateStatisticsYspCnt),
                        value[4] / (float) (validateYspCmt + validateStatisticsYspCnt),
                        value[5] / (float) (validateYspCmt + validateStatisticsYspCnt));
            }
            else {
                System.out.format("%18s%18s%18s%18.3f%18.3f%18.3f%18.3f%n",
                        key, value[0], value[1],
                        value[2] / (float) validateYspCmt,
                        value[3] / (float) validateYspCmt,
                        value[4] / (float) validateYspCmt,
                        value[5] / (float) validateYspCmt);
            }
        }
    }

    /**
     * 这里求的是召回率，所以理所当然的，需要去除不符合序列挖掘条件的（包括防止OOM的和出现次数过少的）
     * @param yspCmt
     * @param validateYspCmt
     * @param validateStatisticsYspCnt
     * @param validateYspSum
     * @param evaluateMap
     */
    private void printRecall(int yspCmt, int validateYspCmt, int validateStatisticsYspCnt, int validateYspSum, Map<String, Integer[]> evaluateMap) {
        System.out.format("\n%18s%18s%18s%18s%18s%n", "Sorting rules", "Recall(1)", "Recall(5)", "Recall(10)", "Recall(50)");
        DecimalFormat decimalFormat = new DecimalFormat("0.00%");
        for (Map.Entry<String, Integer[]> entry : evaluateMap.entrySet()) {
            String key = entry.getKey();
            Integer[] value = entry.getValue();
            System.out.format("%18s%18s%18s%18s%18s%n",
                    key,
                    decimalFormat.format(value[3] / (float) (validateYspCmt)),
                    decimalFormat.format(value[4] / (float) (validateYspCmt)),
                    decimalFormat.format(value[5] / (float) (validateYspCmt)),
                    decimalFormat.format(value[0] / (float) validateYspCmt));
        }
    }

    @Test
    public void testGrammar() {
        System.out.format("%18s%18.2f", 'a', 15.8888);
    }


    public static void main(String[] args) {
        EffectEvaluation effectEvaluation = new EffectEvaluation();
        effectEvaluation.evaluateFromFile("testSet.txt");
    }
}
