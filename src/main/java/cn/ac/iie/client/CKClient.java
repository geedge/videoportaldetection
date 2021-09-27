package cn.ac.iie.client;

import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.Utils.URLHandleUtils;
import cn.ac.iie.mapper.AVDataListMapper;
import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.util.List;

public class CKClient {
    /**
     * 从ClickHouse中音视频表拿到音视频的host，found_time，client_ip
     * 用音视频的client_ip和found_time去查询ClickHouse中全流量表的数据，拿到，其http_url / sni，记录到redis
     * redis设计为哈希表，key为音视频host，value为哈希表，包含音视频出现次数以及每个共现的host出现的次数
     *
     * 策略：
     * 1. 窗口内每个host取distinct，因为入口页面本来就出现得少，不然可能会让广告和图片很多（之所以不排除出现1次以上的，是因为入口的host经过过滤可能和其他一样）
     * 2. 窗口靠前的才是入口页面
     * 3. 正则匹配删除带有host倒数第二次字段为pic，img的
     */
    @Test
    public void yspFromCKStart() {
        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        AVDataListMapper avListMapper = sqlSession.getMapper(AVDataListMapper.class);
        List<AVlog> aVlogList = avListMapper.getAVList();

        List<TrafficLog> trafficLogList;
        long eachFoundTime;
        String eachClient_ip;
        String yspHost;

        for (AVlog eachVideo : aVlogList) {
            eachClient_ip = eachVideo.getClient_ip();
            eachFoundTime = eachVideo.getFound_time();
            yspHost = eachVideo.getUrl();
            if (yspHost.startsWith("200.200.200.200")) continue;

            trafficLogList = avListMapper.getRelationListWithHost(eachFoundTime, eachClient_ip,10);
            String relateHost;
            for (TrafficLog trafficLog : trafficLogList) {
                if (trafficLog.getHttp_host() != null && trafficLog.getHttp_host().trim().length() != 0)
                    relateHost = trafficLog.getHttp_host();
                else if (trafficLog.getSsl_sni() != null && URLHandleUtils.handleSNI(trafficLog.getSsl_sni()).trim().length() != 0)
                    relateHost = URLHandleUtils.handleSNI(trafficLog.getSsl_sni());
                else continue;
                StaticFrequency.update2Redis(yspHost, relateHost);
                System.out.println(yspHost + " " + eachClient_ip + " " + eachFoundTime + " " + relateHost);
            }
        }
        sqlSession.close();
        StaticFrequency.closeSessions();
    }

}
