package cn.ac.iie.intervalStatics;

import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.config.Configurations;
import cn.ac.iie.mapper.AVDataListMapper;
import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.ibatis.session.SqlSession;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MiningPreliminaryPrep {
    /**
     * 希望找到包含90%-95%的的时间窗口
     */
    public void getHTTPTimeWindows() {
        double allInterval = 0;
        List<Integer> intervals;                        // 因为数据库存储发现时间的最小单位是秒，所以这里一定是整数

        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        AVDataListMapper avListMapper = sqlSession.getMapper(AVDataListMapper.class);
        List<AVlog> aVRefererlogList = avListMapper.getAVListWithReferer();

        intervals = new ArrayList<>();
        List<TrafficLog> trafficLogList;
        for(AVlog eachVideo : aVRefererlogList) {
            String eachClient_ip = eachVideo.getClient_ip();
            String eachRefHost = eachVideo.getReferer_host();
            long eachFoundTime = eachVideo.getFound_time();

            // 依次调用，并且只取一个，因为往往多个的时间是一样的
            trafficLogList = avListMapper.getReferPortal(eachFoundTime, eachClient_ip, eachRefHost);
            if (trafficLogList.size() > 0) {
                int eachInterval = (int) (eachFoundTime - trafficLogList.get(0).getCommon_start_time());
                allInterval += eachInterval;
                intervals.add(eachInterval);
            }
        }

        int count = intervals.size();
        Collections.sort(intervals);
        int index = (int) (intervals.size()*0.95);
        System.out.println("共测试" + count + "个，95%的个数为" + index);
        System.out.println("95%的出现间隔为： " + intervals.get(index));
        System.out.println("90%的出现间隔为： " + intervals.get((int) (intervals.size()*0.9)));
        System.out.println("80%的出现间隔为： " + intervals.get((int) (intervals.size()*0.8)));
        System.out.println("平均出现间隔为：" + allInterval / count);
        System.out.println("中位数为：" + intervals.get(intervals.size() / 2));
        System.out.println("最长出现间隔为： " + intervals.get(count-1));
        Configurations.setInterval(String.valueOf(intervals.get(index)));
    }

    /**
     * 生成明文能找到referer的测试集，每一行如下格式：
     * client_ip, url, 经过处理的referer, fount_time, refer的start_time, interval
     * Node：生成训练数据集的函数不能再jar包中执行，因为路径问题
     */
    public void generateTrainSet(String dataExp) {
        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        AVDataListMapper avListMapper = sqlSession.getMapper(AVDataListMapper.class);

        // 这里sql在特定场景应该改为distinct，虽然不distinct的话url不同，但是计算的时候是根据host来的
        List<AVlog> aVRefererlogList;
        if (dataExp.toLowerCase().equals("web")) {
            aVRefererlogList = avListMapper.getAVListReferForWeb();
        }
        else {
            aVRefererlogList = avListMapper.getAVListWithReferer();
        }
        Set<String> trainSet = new HashSet<>();

        System.out.println(String.format("目前共%s有referer", aVRefererlogList.size()));
        List<TrafficLog> trafficLogList;
        for(AVlog eachVideo : aVRefererlogList) {
            String eachClient_ip = eachVideo.getClient_ip();

            // 这里的refer在查询的时候已经经过了CK的domain函数处理
            String eachRefHost = eachVideo.getReferer_host();
            long eachFoundTime = eachVideo.getFound_time();

            trafficLogList = avListMapper.getReferPortal(eachFoundTime, eachClient_ip, eachRefHost);
            if (trafficLogList.size() > 0) {
                int eachInterval = (int) (eachFoundTime - trafficLogList.get(0).getCommon_start_time());
                String eachVideoUrl = eachVideo.getUrl();
                long referTime = trafficLogList.get(0).getCommon_start_time();
                String line = eachClient_ip + " " +
                        eachVideoUrl + " " +
                        eachRefHost + " " +
                        eachFoundTime + " " +
                        referTime + " " +
                        eachInterval;
                trainSet.add(line);
            }
        }
        sqlSession.close();

        System.out.println("候选集大小为: " + trainSet.size());
        String outPath = "./src/main/resources/testSet" + dataExp + ".txt";
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(outPath), StandardCharsets.UTF_8)) {
            for (String line: trainSet) {
                writer.write(line + "\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 开始时间
        long stime = System.currentTimeMillis();
        MiningPreliminaryPrep miningPreliminaryPrep = new MiningPreliminaryPrep();
        miningPreliminaryPrep.generateTrainSet("ALL");
        // 结束时间
        long etime = System.currentTimeMillis();
        System.out.printf("执行时长：%f 分钟.", (etime - stime) / 60000.0);
    }
}
