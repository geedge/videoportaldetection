package cn.ac.iie.pojo;

import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.mapper.SSLListMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.util.List;

public class SSLlogTest {
    @Test
    public void test() {
        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        //下面生成了一个代理类
        SSLListMapper sslListMapper = sqlSession.getMapper(SSLListMapper.class);
        // 裂开了，莫非要用字符串或者bigDecimal去存？
        List<SSLlog> ssLlogList = sslListMapper.getRelationList(1615961835, 5);
//        UserMapper userMapper = sqlSession.getMapper(UserMapper.class);
//        User user = userMapper.getUserById(1);
//        System.out.println(user);
        for (int i = 0; i < ssLlogList.size(); i++) {
            System.out.println(ssLlogList.get(i).toString());
        }
        sqlSession.close();
    }
}