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

    /**
     * Node：有时候会出现182.254.52.218/amobile.music.tc.qq.com这样的形式，需要特殊处理
     * @param url 完整的url地址
     * @return 作为数据挖掘的host
     */
    public static String getFirstURLPath(String url) {
        String[] yspHostList = url.split("/");
        if (yspHostList.length >= 2) {
            if (isCorrectIp(yspHostList[0]) && isDomainCrude(yspHostList[1]))
                return yspHostList[1];
            else {
                return yspHostList[0];
            }
        }
        return url;
    }

    public static boolean isCorrectIp(String ipString) {
        String ipRegex = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";	//IP地址的正则表达式
        //如果前三项判断都满足，就判断每段数字是否都位于0-255之间
        if (ipString.matches(ipRegex)) {
            String[] ipArray = ipString.split("\\.");
            for (int i = 0; i < ipArray.length; i++) {
                int number = Integer.parseInt(ipArray[i]);
                //4.判断每段数字是否都在0-255之间
                if (number <0||number>255) {
                    return false;
                }
            }
            return true;
        }
        else {
            return false;	//如果与正则表达式不匹配，则返回false
        }
    }

    private static boolean isDomainCrude(String isString) {
        String[] strSpl = isString.split("\\.");
        if (strSpl.length == 1)
            return false;
        char[] chars = strSpl[strSpl.length - 1].toCharArray();
        boolean isPhontic;
        for (char aChar : chars) {
            isPhontic = (aChar >= 'a' && aChar <= 'z') || (aChar >= 'A' && aChar <= 'Z');
            if (!isPhontic) {
                return false;
            }
        }
        return true;
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
