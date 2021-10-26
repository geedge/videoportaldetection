package cn.ac.iie.mapper;

import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface AVDataListMapper {
    public List<TrafficLog> getRelationList(@Param("found_time") long found_time, @Param("intervalTime") int intervalTime);

    public List<TrafficLog> getRelationListWithHost(@Param("found_time") long found_time,
                                                    @Param("client_ip") String client_ip,
                                                    @Param("startInterval") int startInterval,
                                                    @Param("endInterval") int endInterval);

    public List<AVlog> getAVList();

    public List<AVlog> getAVListWithReferer();

    public List<TrafficLog> getReferPortal(@Param("found_time") long found_time,
                                           @Param("client_ip") String client_ip,
                                           @Param("host") String refer);

    public List<AVlog> statisticsUA();

    public int cumulativeData(@Param("likeURL") String urlPath, @Param("effectiveTime") long effectiveTime);

    public List<AVlog> getHistoryHost(@Param("likeURL") String host, @Param("rawLimit") Integer rawLimit);

    public List<AVlog> getHostNeedMining(@Param("taskDays") Integer taskTime);
}
