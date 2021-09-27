package cn.ac.iie.client;


import org.junit.Test;

import static cn.ac.iie.Utils.URLHandleUtils.handleSSLCert;

public class StaticFrequencyTest {
    @Test
    public void testURLPattern() {
        String cert = "{\"sub\":\"mmbiz.qpic.cn;Shenzhen Tencent Computer Systems Company Limited;CN\",\"issue_cname\":\"Secure Site CA G2\",\"sub_cname\":\"mmbiz.qpic.cn\",\"sub_organize\":\"Shenzhen Tencent Computer Systems Company Limited\",\"expire_time\":\"210816120000Z\",\"serial_number\":\"07709816D4AFF02C59EE6C635AD0CF58\",\"version\":\"v3\",\"issuer\":\"Secure Site CA G2;DigiCert Inc;US\",\"issue_country\":\"US\",\"sub_country\":\"CN\",\"start_time\":\"200629000000Z\",\"issue_organize\":\"DigiCert Inc\",\"algorithm_id\":\"1.2.840.113549.1.1.11\"}";
        System.out.println(handleSSLCert(cert));
    }
}