package cn.ac.iie.mapper;

import cn.ac.iie.pojo.AVlog;
import cn.ac.iie.pojo.TrafficLog;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface AVDataListMapper {
    public List<TrafficLog> getRelationList(@Param("found_time") long found_time, @Param("intervalTime") int intervalTime);

    /**
     * 根据IP和时间来获得当时音视频的上下文，这里会按照出现先后进行倒序排序，原因是为了去除YSP本身对应的host
     * @param found_time 当时音视频的记录时间，在全流量表中对应common_start_time
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

    /**
     * 判断在有效时间内这个音视频出现的次数是否超过了最低阈值
     * @param urlPath 音视频的URL
     * @param effectiveTime 有效时间
     * @return 有效时间内出现的次数
     */
    public int cumulativeData(@Param("likeURL") String urlPath, @Param("effectiveTime") long effectiveTime);

    /**
     * 从音视频表查找曾经出现过的视频
     * Note:
     *      1. 这里用的是host，在实际中明文传输的视频应该用url来唯一对应，加密的视频用sni来对应
     *      2. 目前用like的话来大数据场景下会有误差，比如url中后面字段才有host的字符，这种情况不一定是一个音视频
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

    /**
     * 无论是否有referer，无论在时间窗口内是否出现入口，都返回
     * Note:
     * 1. 200.200.200.200不返回
     * 2. 经过domain函数去重，虽然写入AVlog的时候会转换url，但是没有去重，所以这里sql还是需要用domain函数
     * 3. 直接用音视频表过滤超过5次的
     * @return 去重后的所有音视频流量
     */
    public List<AVlog> getUniqAVListForAll();
}
