package cn.ac.iie.intervalStatics;

import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.Utils.URLHandleUtils;
import cn.ac.iie.mapper.AVDataListMapper;
import cn.ac.iie.pojo.AVlog;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class Statistics {
    Map<String, Integer> UAMap;

    @Test
    public void staUserAgent() {
        long stime = System.currentTimeMillis();

        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        AVDataListMapper avListMapper = sqlSession.getMapper(AVDataListMapper.class);
        List<AVlog> avWithUA = avListMapper.statisticsUA();
        int allSize = avWithUA.size();

        UAMap = new HashMap<>();

        /*
        * 具体规则如下：
        *   1. 按照_和-进行拆分，取前1个
        *   2. 按照(或/拆分
        *   3. 如果是Mozilla，则保留拆分后列表前3个值
        *   实现为从前往后遍历，遇到 _ - ( / 就拆分
        * */
        for(AVlog eachAV: avWithUA) {
            String UA = eachAV.getUser_agent();
            UA = URLHandleUtils.handleUA(UA);
            if (UAMap.containsKey(UA)) {
                UAMap.put(UA, UAMap.get(UA)+1);
            }
            else UAMap.put(UA, 1);
        }
        System.out.println("UA处理完毕，共" + UAMap.size() + "条");

        List<Map.Entry<String, Integer>> infolds = new ArrayList<>(UAMap.entrySet());
        infolds.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("src/main/resources/UACount.txt"));
            DecimalFormat decimalFormat = new DecimalFormat("#.0000");
            for(Map.Entry<String, Integer> each : infolds) {
                out.write(each.getKey() + "\t" + each.getValue() + "\t" + Float.parseFloat(decimalFormat.format((float)each.getValue()/allSize))*100 + "%\n");
            }
            out.write("不同UA个数为：" + UAMap.size() + "\n");
            out.write("UA总数为：" + allSize);
            out.close();
            System.out.println("User_agent统计数据已写入UACount.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

        long etime = System.currentTimeMillis();
        System.out.printf("执行时长：%d 毫秒.", (etime - stime));
    }
}
