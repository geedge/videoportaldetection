package cn.ac.iie.mapper;

import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface AVDataListMapper {
    public List<TrafficLog> getRelationList(@Param("found_time") long found_time, @Param("intervalTime") int intervalTime);

    /**
     * 根据IP和时间来获得当时音视频的上下文
     * @param found_time 当时音视频的记录时间
     * @param client_ip 当时音视频的client_ip
     * @param startInterval 时间窗口左边界
     * @param endInterval 时间窗口右边界
     * @return 音视频上下文的host
     */
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

    /**
     * 从音视频表查找曾经出现过的视频
     * Node: 这里用的是host，在实际中明文传输的视频应该用url来唯一对应，加密的视频用sni来对应
     * @param host YSP的host
     * @param rawLimit 最多找多少条
     * @return 曾经出现过的音视频
     */
    public List<AVlog> getHistoryHost(@Param("likeURL") String host, @Param("rawLimit") Integer rawLimit);

    public List<AVlog> getHostNeedMining(@Param("taskDays") Integer taskTime);

    /**
     * 与 getAVListWithReferer 的区别在于是否过滤掉了app访问的视频
     * @return 浏览器访问的未加密有referer视频列表
     * Node:
     *  1.目前判断浏览器的流量方法非常原始，仅仅是通过chrome的高版本来实现，在实际工程中不可如此
     *  2.过滤掉了200.200.200.200
     */
    public List<AVlog> getAVListReferForWeb();
}
