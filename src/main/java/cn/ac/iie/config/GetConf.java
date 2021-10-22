package cn.ac.iie.config;

import java.io.Serializable;

public class GetConf implements Serializable {
    private static final long serialVersionUID = 1641809911536544588L;

    public static final String BOOTSTRAP_SERVERS = Configurations.getStringProperty(0, "bootstrap.servers");
    public static final String KAFKA_AV_TOPIC = Configurations.getStringProperty(0, "kafka.av.topic");
}
