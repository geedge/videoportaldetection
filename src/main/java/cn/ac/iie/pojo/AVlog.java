package cn.ac.iie.pojo;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import static cn.ac.iie.Utils.URLHandleUtils.yspHostHandle;

@ToString
public class AVlog {
    @Getter
    @Setter
    private String client_ip;

    @Getter
    @Setter
    private String server_ip;

    @Getter
    private String url;

    public void setUrl(String url) {
        this.url = yspHostHandle(url);
    }

    @Getter
    @Setter
    private String referer;

    @Getter
    @Setter
    private long recv_time;

    @Getter
    @Setter
    private long found_time;

    @Getter
    @Setter
    private long expire_time;

    @Getter
    @Setter
    private String user_agent;

}
