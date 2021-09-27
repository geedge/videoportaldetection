package cn.ac.iie.pojo;


import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.mapper.HTTPListMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;

public class HTTPlogTest {
    @Test
    public void test() {
        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        HTTPListMapper httpListMapper = sqlSession.getMapper(HTTPListMapper.class);

        List<HTTPlog> httPlogList = httpListMapper.getRelationList(new BigInteger("1623056230000"), 5000);
        for (HTTPlog eachHTTPLog : httPlogList) {
            System.out.println(eachHTTPLog.toString());
        }
        sqlSession.close();
    }
}