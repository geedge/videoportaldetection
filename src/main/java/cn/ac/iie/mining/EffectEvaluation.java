package cn.ac.iie.mining;


import cn.ac.iie.Utils.JedisPoolUtils;
import cn.ac.iie.Utils.URLHandleUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;

public class EffectEvaluation {
    private static Logger logger = LoggerFactory.getLogger(EffectEvaluation.class);

    /**
     * 评估指标：平均出现出现index最小的顺序，最小顺序中在第1次出现的占比，在前5次出现的占比，在前10次出现的占比，不出现的占比
     * 计算：出现次数，不出现的次数，每个方案出现的index之和，每个方案第1次出现的次数，每个方案在前5次出现的次数，每个方案在前10次出现的次数
     */
    public void evaluateFromFile() {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream("testSet.txt")){
            if (is == null) {
                System.out.println("评估文件不存在");
                System.exit(0);
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            MiningService miningService = new MiningService();
            int validateYspCmt = 0;
            int validateYspSum = 0;
            int yspCmt = 0;
            Map<String, Integer[]> evaluateMap = new HashMap<>();

            int testCnt = 0;

            // client_ip, url, 经过处理的referer, fount_time, refer的start_time, interval
            String line = "";
            while((line = br.readLine())!=null) {
                String[] item = line.split(" ");
                String cIp = item[0];
                String yspHost = URLHandleUtils.getFirstURLPath(item[1]);
                String refererHost = item[2];
                int[] resultIndex = itemEvaluate(cIp, yspHost, refererHost, miningService);

                yspCmt += 1;
                if (resultIndex[0] == -1) continue;
                validateYspCmt += 1;
                validateYspSum += resultIndex[6];
                accumulateResult(evaluateMap, "statistic", resultIndex[1]);
                accumulateResult(evaluateMap, "rule1", resultIndex[2]);
                accumulateResult(evaluateMap, "sta_rule1", resultIndex[3]);
                accumulateResult(evaluateMap, "rule2", resultIndex[4]);
                accumulateResult(evaluateMap, "sta_rule1_rule2", resultIndex[5]);

                testCnt += 1;
                if (testCnt % 5 == 0) {
                    logger.warn(String.format("目前处理第%d条", testCnt));
                }
            }
            printEvaluate(yspCmt, validateYspCmt, validateYspSum, evaluateMap);
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
    public int[] itemEvaluate(String clientIP, String yspHost, String refererHost, MiningService miningService) {
        Jedis jedis = JedisPoolUtils.getJedis();
        if (jedis != null && jedis.exists(yspHost)) {
            String statisticCandidate = jedis.hget(yspHost, "statistic");
            String rule1Candidate = jedis.hget(yspHost, "rule1");
            String staRule1Candidate = jedis.hget(yspHost, "sta_rule1");
            String rule2Candidate = jedis.hget(yspHost, "rule2");
            String prefixCandidate = jedis.hget(yspHost, "sta_rule1_rule2");
            if (prefixCandidate == null || statisticCandidate == null || rule1Candidate == null || staRule1Candidate == null || rule2Candidate == null) {
                // 此时计算
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
                        statisticCandidate.split(", ").length
                };
            }
        }
        else {
            if (!miningService.isMore10(yspHost)) {
                logger.warn(String.format("%s数据过少，不需要进行挖掘", yspHost));
                return new int[]{-1, -1, -1, -1, -1, -1, -1};
            }
            int[] resultIndex = mining2Evaluate(yspHost, refererHost, miningService, jedis);
            JedisPoolUtils.returnResource(jedis);
            return resultIndex;
        }
    }

    private int[] mining2Evaluate(String yspHost, String refererHost, MiningService miningService, Jedis jedis) {
        ArrayList<Row> data = miningService.buildInput(yspHost);
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
        }
        return new int[]{
                1,
                judgeIndex(candidateList.get(0), refererHost),
                judgeIndex(candidateList.get(1), refererHost),
                judgeIndex(candidateList.get(2), refererHost),
                judgeIndex(candidateList.get(3), refererHost),
                judgeIndex(candidateList.get(4), refererHost),
                candidateList.get(0).split(", ").length
        };
    }

    /**
     * TODO:这里判断容易出问题，需要验证一下
     */
    private int judgeIndex(String candidate, String refererHost) {
        String[] candidateArray = candidate.substring(1, candidate.length()-1).split(", ");
        for (int i = 0; i < candidateArray.length; i++) {
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

    private void printEvaluate(int yspCmt, int validateYspCmt, int validateYspSum, Map<String, Integer[]> evaluateMap) {
        System.out.println(String.format("在一定时间窗口且存在referer的YSP总数为%s，其中符合挖掘条件的个数为%s，候选集平均长度为%s",
                yspCmt, validateYspCmt, validateYspSum /  (float) validateYspCmt));
        System.out.format("%18s%18s%18s%18s%18s%18s%18s%n", "Sorting rules", "Occurrences", "No-occurrence", "Index avg", "Prop pos=1", "Prop pos<5", "Prop pos<10");
        for (Map.Entry<String, Integer[]> entry : evaluateMap.entrySet()) {
            String key = entry.getKey();
            Integer[] value = entry.getValue();
            System.out.format("%18s%18s%18s%18s%18s%18s%18s%n",
                    key, value[0], value[1],
                    value[2] / (float) validateYspCmt,
                    value[3] / (float) validateYspCmt,
                    value[4] / (float) validateYspCmt,
                    value[5] / (float) validateYspCmt);
        }
    }

    @Test
    public void grammarTest() {
        HashMap<String, Integer[]> map = new HashMap<>();
        Integer[] testA = new Integer[]{0,0,0};
        map.put("testa", testA);
        Integer[] arraya = map.get("testa");
        arraya[1] += 1;
        for (Integer each: map.get("testa")) {
            System.out.println(each);
        }
        System.out.format("%8s%8d%8s%n", "string1", 1, "string2");
        System.out.format("%12s%12s%12s%12s%12s%12s%12s%n", "方式", "出现次数", "不出现次数", "平均Index", "第1次出现占比", "前5次出现占比", "前10次出现占比");
        System.out.format("%12s%12d%12d%12f%12f%12f%12f%n",
                "key", 0, 1, 1.1, 1.2, 1.3, 1.4);
        File f = new File(this.getClass().getResource("").getPath());
        System.out.println(f);
    }

    public static void main(String[] args) {
        EffectEvaluation effectEvaluation = new EffectEvaluation();
        effectEvaluation.evaluateFromFile();
    }
}
