package cn.ac.iie.pojo;

import cn.ac.iie.Utils.SqlSessionFactoryUtils;
import cn.ac.iie.mapper.AVDataListMapper;
import cn.ac.iie.mapper.SSLListMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

import java.util.List;

public class TrafficLogTest {
    @Test
    public void test() {
        SqlSession sqlSession = SqlSessionFactoryUtils.getSqlSessionFactory().openSession();
        AVDataListMapper avDataListMapper = sqlSession.getMapper(AVDataListMapper.class);
        List<TrafficLog> trafficLogList = avDataListMapper.getRelationList(1624602265, 5);
        for (int i = 0; i < trafficLogList.size(); i++) {
            System.out.println(trafficLogList.get(i).toString());
        }

        List<AVlog> avList = avDataListMapper.getAVList();
        for (int i = 0; i < avList.size(); i++) {
            System.out.println(avList.get(i).toString());
        }
        sqlSession.close();
    }
}