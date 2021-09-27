package cn.ac.iie.Utils;

import org.junit.Test;

public class URLHandleUtilsTest {
    @Test
    public void test() {
        String referer1 = "http://www.adgoo.cn/miniPage/index.html";
        String referer2 = "182.254.52.218";
        System.out.println(URLHandleUtils.handleReferer(referer1));
    }
}