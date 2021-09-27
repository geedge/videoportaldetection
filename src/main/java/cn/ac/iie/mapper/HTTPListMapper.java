package cn.ac.iie.mapper;

import cn.ac.iie.pojo.HTTPlog;
import org.apache.ibatis.annotations.Param;

import java.math.BigInteger;
import java.util.List;

public interface HTTPListMapper {
    // http表的单位是毫秒，ssl的是秒
    public List<HTTPlog> getRelationList(@Param("found_time") BigInteger found_time, @Param("intervalTime") int intervalTime);
}
