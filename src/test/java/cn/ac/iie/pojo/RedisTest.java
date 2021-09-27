package cn.ac.iie.pojo;

import cn.ac.iie.Utils.JedisPoolUtils;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class RedisTest {
    @Test
    public void testJedis() {
        Jedis jedis = new Jedis("192.168.10.28",6379);
        System.out.println("连接成功");
        //查看服务是否运行
        System.out.println("服务正在运行: "+jedis.ping());
        try {
            jedis = JedisPoolUtils.getJedis();
            System.out.println("连接池正常");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
