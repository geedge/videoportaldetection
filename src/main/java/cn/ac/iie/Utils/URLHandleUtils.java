package cn.ac.iie.Utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLHandleUtils {
    private static Pattern hostPattern = Pattern.compile("\\.\\w+\\.\\w+;");

    public static String yspHostHandle(String yspHost) {
        String[] yspHostList = yspHost.split("/");
        if (yspHostList.length >= 2)
            return yspHostList[0] + "/" + yspHostList[1];
        else return yspHost;
    }

    public static String getFirstURLPath(String url) {
        String[] yspHostList = url.split("/");
        if (yspHostList.length >= 1)
            return yspHostList[0];
        else return url;
    }

    public static String handleSNI(String thisHost) {
        String[] strList = thisHost.split("\\.");
        int len = strList.length;
        if (strList.length >= 2) return strList[len-2] + "." + strList[len-1];
        return thisHost;
    }

    // 获取证书里面host
    public static String handleSSLCert(String cert) {
        Matcher m = hostPattern.matcher(cert);
        while (m.find()) {
            if (m.group(0) != null) {
                String host = m.group(0);
                return host.substring(1, host.length()-1);
            }
        }
        return null;
    }

    /**
     * 传入ip时返回ip，传入http://www.adgoo.cn/miniPage/index.html时返回www.adgoo.cn
     */
    public static String handleReferer(String oldReferer) {
        if (oldReferer.startsWith("http")) {
            String[] strList = oldReferer.split("/");
            if (strList.length > 2) return strList[2];
        }
        return oldReferer;
    }

    public static String handleUA(String user_agent) {
        if (user_agent.startsWith("Mozilla")) {
            String[] list = user_agent.split("/");
            if (list.length >= 3) return list[0]+"/"+list[1]+"/"+list[2];
            else return user_agent;
        }
        else {
            for (int i = 1; i < user_agent.length(); i++) {
                if (user_agent.charAt(i) == '_' || user_agent.charAt(i) == '-'
                        || user_agent.charAt(i) == '(' || user_agent.charAt(i) == '/') {
                    return user_agent.substring(0, i);
                }
            }
        }
        return user_agent;
    }
}
