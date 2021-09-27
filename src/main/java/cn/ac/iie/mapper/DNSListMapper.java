package cn.ac.iie.mapper;

import cn.ac.iie.pojo.DNSlog;
import java.util.List;

public interface DNSListMapper {
    public List<DNSlog> getRelationList(int found_time);
}
