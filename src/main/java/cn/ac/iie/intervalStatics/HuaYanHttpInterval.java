package cn.ac.iie.intervalStatics;

import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.config.Configurations;
import cn.ac.iie.mapper.AVDataListMapper;
import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HuaYanHttpInterval {
    private double allInterval = 0;
    private int count = 0;
    // 因为数据库存储发现时间的最小单位是秒，所以这里一定是整数
    private List<Integer> intervals;

    // 0706完成这一部分

    /**
     * 希望找到包含90%-95%的的时间窗口
     */
    @Test
    public void getHTTPTimeWindows() {

        // 开始时间
        long stime = System.currentTimeMillis();

        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        AVDataListMapper avListMapper = sqlSession.getMapper(AVDataListMapper.class);
        List<AVlog> aVRefererlogList = avListMapper.getAVListWithReferer();

        intervals = new ArrayList<>();
        List<TrafficLog> trafficLogList;
        for(AVlog eachVideo : aVRefererlogList) {
            String eachClient_ip = eachVideo.getClient_ip();
            String eachRef = eachVideo.getReferer();
            long eachFoundTime = eachVideo.getFound_time();

            // 依次调用，并且只取一个，因为往往多个的时间是一样的
            trafficLogList = avListMapper.getReferPortal(eachFoundTime, eachClient_ip, eachRef);
            if (trafficLogList.size() > 0) {
                int eachInterval = (int) (eachFoundTime - trafficLogList.get(0).getCommon_start_time());
                allInterval += eachInterval;
                count += 1;
                intervals.add(eachInterval);
            }
        }

        Collections.sort(intervals);
        int index = (int) (intervals.size()*0.95);
        System.out.println("共测试" + intervals.size() + "个，95%的个数为" + index);
        System.out.println("95%的出现间隔为： " + intervals.get(index));
        System.out.println("90%的出现间隔为： " + intervals.get((int) (intervals.size()*0.9)));
        System.out.println("80%的出现间隔为： " + intervals.get((int) (intervals.size()*0.8)));
        System.out.println("平均出现间隔为：" + allInterval / count);
        System.out.println("中位数为：" + intervals.get(intervals.size() / 2));
        System.out.println("最长出现间隔为： " + intervals.get(count-1));
        Configurations.setInterval(String.valueOf(intervals.get(index)));

        // 结束时间
        long etime = System.currentTimeMillis();
        System.out.printf("执行时长：%d 毫秒.", (etime - stime));
    }
}
