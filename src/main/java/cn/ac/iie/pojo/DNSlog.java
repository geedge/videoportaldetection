package cn.ac.iie.pojo;

import lombok.Getter;
import lombok.Setter;

//获取client_ip, client_port, found_time, recv_time, server_ip, server_port, url字段值
public class DNSlog {

    @Getter
    @Setter
    private String d_ip;

    @Getter
    @Setter
    private String s_ip;

    @Getter
    @Setter
    private String d_port;

    @Getter
    @Setter
    private String s_port;

    @Getter
    @Setter
    private long found_time;

    @Setter
    @Getter
    private int recv_time;

    @Getter
    @Setter
    private String url;
}
