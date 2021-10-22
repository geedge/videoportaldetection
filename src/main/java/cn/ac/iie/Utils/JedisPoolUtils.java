package cn.ac.iie.Utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Properties;

public class JedisPoolUtils {

    // Redis服务器IP
    private static String ADDR = "127.0.0.1";
    // Redis的端口号
    private static int PORT = 6379;
    // 可用连接实例的最大数目，默认值为8；
    // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static int MAX_ACTIVE = 1024;
    // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static int MAX_IDLE = 200;
    // 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int MAX_WAIT = 10000;
    // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static boolean TEST_ON_BORROW = true;
    //return 一个jedis实例给pool时，是否检查连接可用性（ping()）
    private static boolean TEST_ON_RETURN = true;
    private static JedisPool jedisPool = null;

    static {
        Properties props = new Properties();
        try {
            // 在jar包中调用resources中的资源，要考虑路径的问题，用getClassLoader读取工作目录，由于jar属于压缩文件，所以不能用路径来读取，会出现jar!
            //props.load(new java.io.FileInputStream(JedisPoolUtils.class.getClassLoader().getResource("redis.properties").getFile()));
            props.load(JedisPoolUtils.class.getClassLoader().getResourceAsStream("redis.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        ADDR = props.getProperty("redisHost");
        PORT = Integer.parseInt(props.getProperty("redisPort"));
    }

    /**
     * 初始化Redis连接池
     */
    public static JedisPool getJedisPoolInstance(){
        if(null == jedisPool){
            //同步锁
            synchronized (JedisPoolUtils.class)
            {
                if(null == jedisPool)
                {
                    //jedis连接池的配置
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(MAX_ACTIVE);
                    poolConfig.setMaxIdle(MAX_IDLE);
                    poolConfig.setMaxWaitMillis(MAX_WAIT);
                    poolConfig.setTestOnBorrow(TEST_ON_BORROW);
                    poolConfig.setTestOnReturn(TEST_ON_RETURN);
                    jedisPool = new JedisPool(poolConfig, ADDR, PORT);
                }
            }
        }
        return jedisPool;
    }

    /**
     * 获取Jedis实例
     *
     * @return
     */
    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool == null) {
                JedisPool jedisPool = JedisPoolUtils.getJedisPoolInstance();
            }
            Jedis resource = jedisPool.getResource();
            return resource;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 释放jedis资源
     *
     * @param jedis
     */
    public static void releaseResource(final Jedis jedis) {
        if (jedis != null) {
            jedisPool.close();
        }
    }

    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }
}
