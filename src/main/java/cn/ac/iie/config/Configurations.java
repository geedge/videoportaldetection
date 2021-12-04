package cn.ac.iie.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class Configurations {
    private static Properties propCommon = new Properties();
    private static Logger logger = LoggerFactory.getLogger(Configurations.class);

    static {
        try {
            propCommon.load(Configurations.class.getClassLoader().getResourceAsStream("video_data.properties"));
            logger.warn("配置文件video_data.properties加载成功");
        } catch (Exception e) {
            propCommon = null;
            System.err.println("配置加载失败");
        }
    }

    public static String getStringProperty(Integer type, String key) {
        if(type == 0 && propCommon.getProperty(key) != null){
            return propCommon.getProperty(key);
        } else {
            return null;
        }
    }

    public static Integer getIntProperty(Integer type, String key) {
        if(type == 0 && propCommon.getProperty(key) != null){
            return Integer.parseInt(propCommon.getProperty(key));
        } else {
            return null;
        }
    }

    public static Double getDoubleProperty(Integer type, String key) {
        if(type == 0 && propCommon.getProperty(key) != null){
            return Double.parseDouble(propCommon.getProperty(key));
        } else {
            return null;
        }
    }

    public static void setInterval(String intervalWindows) {
        propCommon.setProperty("interval", intervalWindows);
        FileOutputStream oFile = null;
        try {
            oFile = new FileOutputStream("./data/video_data.properties");
            propCommon.store(oFile, "statistic interval windows");
            oFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("interval 更新成功");
    }
}
