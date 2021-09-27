package cn.ac.iie.pojo;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigInteger;

@ToString
public class HTTPlog {

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

    /*
    由于这里found_time过长，所以只能定义为BigInteger,同时mapper的传参也需要修改喔～
     */
    @Getter
    @Setter
    private BigInteger found_time;

    @Setter
    @Getter
    private BigInteger recv_time;

    @Getter
    @Setter
    private String url;
}
