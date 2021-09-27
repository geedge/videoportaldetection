package cn.ac.iie.mapper;

import cn.ac.iie.pojo.SSLlog;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface SSLListMapper {
    public List<SSLlog> getRelationList(@Param("create_time") long create_time, @Param("intervalTime") int intervalTime);

    public List<SSLlog> getRelationListByCip(@Param("s_ip") String s_ip, @Param("create_time") long create_time, @Param("intervalTime") int intervalTime);
}
