package cn.ac.iie.client;

import cn.ac.iie.Utils.JedisPoolUtils;
import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.mapper.SSLListMapper;
import cn.ac.iie.pojo.SSLlog;
import org.apache.ibatis.session.SqlSession;
import org.apache.kafka.common.protocol.types.Field;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.ac.iie.Utils.URLHandleUtils.*;

/**
 * kafka得到音视频的url，client_ip，found_time
 * 去clickhouse去查询c_ip相同，found_time前后波动5秒的value的host
 * 更新redis中的值，key是音视频的url，value设置为组合类型
 */
public class StaticFrequency {

    private static SqlSession sqlSession;
    private static Jedis jedis;

    static {
        sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        if (jedis == null) {
            JedisPoolUtils.getJedisPoolInstance();
            jedis = JedisPoolUtils.getJedis();
        }
    }

    public static void closeSessions() {
        sqlSession.close();
        JedisPoolUtils.returnResource(jedis);
    }

    /**
     * 这个函数用于获得ClickHouse中相同clietn ip的音视频前后共现的host信息
     * @param s_ip kafka传参 source ip
     * @param create_time 音视频发现时间
     * @param intervalTime 前后多少秒时间
     */
    public void relationSSL2Redis(String yspHost, String s_ip, long create_time, int intervalTime) {
        yspHost = yspHostHandle(yspHost);

        SSLListMapper sslListMapper = sqlSession.getMapper(SSLListMapper.class);
        List<SSLlog> ssLlogList = sslListMapper.getRelationListByCip(s_ip, create_time, intervalTime);

        List<String> hostList = new ArrayList<>();
        String thisHost;
        for (SSLlog ssLlog : ssLlogList) {
            thisHost = ssLlog.getSni();
            if (thisHost == null || thisHost.length() == 0)
                thisHost = handleSSLCert(ssLlog.getServer_cert());
            else thisHost = handleSNI(thisHost);
            if (thisHost != null && thisHost.length() != 0) hostList.add(thisHost);
        }
//        update2Redis(yspHost, hostList);
        System.out.println(yspHost + " " + s_ip + " " + create_time + " " + hostList.toString());
    }

    /**
     * Redis数据类型设计：
     * value为哈希表，key为音视频的sni
     * value中存音视频的sni出现的次数以及共现的所有的host的次数
     */
    public static void updateList2Redis(String yspHost, List<String> coHostList) {
        if (jedis.exists(yspHost))
            jedis.hincrBy(yspHost, "occurTime", 1);
        else jedis.hset(yspHost, "occurTime", String.valueOf(1));
        for (String eachHost : coHostList) {
            if (jedis.hexists(yspHost, eachHost)) jedis.hincrBy(yspHost, eachHost, 1);
            else jedis.hset(yspHost, eachHost, String.valueOf(1));
        }
    }

    public static void update2Redis(String yspHost, String eachHost) {
        // 因为是做实验，所以这里先flushDB
        jedis.flushDB();
        if (jedis.exists(yspHost))
            jedis.hincrBy(yspHost, "occurTime", 1);
        else jedis.hset(yspHost, "occurTime", String.valueOf(1));
        if (jedis.hexists(yspHost, eachHost)) jedis.hincrBy(yspHost, eachHost, 1);
        else jedis.hset(yspHost, eachHost, String.valueOf(1));
    }

    /**
     * 用于清空redis中已有的音视频数据
     */
    private void clearRedisYSPHost(String yspHost) {
        //
    }
}
