package cn.ac.iie.pojo;

import cn.ac.iie.Utils.URLHandleUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * DNS的记录在connection_record_log_local表全部为空
 */
@ToString
public class TrafficLog {
    @Getter
    @Setter
    private String common_client_ip;

    @Getter
    @Setter
    private String common_client_port;

    @Getter
    @Setter
    private String common_server_ip;

    @Getter
    @Setter
    private String common_server_port;

    @Getter
    @Setter
    private long common_recv_time;

    /**
     * common_start_time是前端收到数据的时间
     */
    @Setter
    @Getter
    private long common_start_time;

    @Getter
    private String http_url;

    public void setHttp_url(String http_url) {
        this.http_url = URLHandleUtils.yspHostHandle(http_url);
    }

    @Getter
    @Setter
    private String http_host;

    @Getter
    @Setter
    private String http_domain;

    @Getter
    @Setter
    private String ssl_sni;

    @Getter
    @Setter
    private String ssl_cn;

    /*
     * request body和response body在后续可能会用到
     */
    private String http_request_body;
    private String http_response_body;
    private String referer;
    private String cookie;

}
