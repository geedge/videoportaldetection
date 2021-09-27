package cn.ac.iie.pojo;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class SSLlog {

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
    private long create_time;

    @Setter
    @Getter
    private long last_time;

    @Getter
    @Setter
    private String sni;

    @Getter
    @Setter
    private String server_cert;
}
