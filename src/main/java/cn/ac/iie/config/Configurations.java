package cn.ac.iie.config;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class Configurations {
    private static Properties propCommon = new Properties();

    static {
        try {
            propCommon.load(Configurations.class.getClassLoader().getResourceAsStream("video_data.properties"));
            System.out.println("video_data.properties加载成功");
        } catch (Exception e) {
            propCommon = null;
            System.err.println("配置加载失败");
        }
    }

    public static String getStringProperty(Integer type, String key) {
        if(type == 0){
            return propCommon.getProperty(key);
        } else {
            return null;
        }
    }

    public static Integer getIntProperty(Integer type, String key) {
        if(type == 0){
            return Integer.parseInt(propCommon.getProperty(key));
        } else {
            return null;
        }
    }

    public static void setInterval(String intervalWindows) {
        propCommon.setProperty("interval", intervalWindows);
        FileOutputStream oFile = null;
        try {
            oFile = new FileOutputStream("video_data.properties");
            propCommon.store(oFile, "statistic interval windows");
            oFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("interval 更新成功");
    }
}
