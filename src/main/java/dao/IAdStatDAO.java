package dao;

import domain.AdStat;

import java.util.List;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:11
 * @Description: 广告实时统计DAO接口
 */
public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStats);
}
