package dao;

import domain.AdUserClickCount;

import java.util.List;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:12
 * @Description: 用户广告点击量DAO接口
 */
public interface IAdUserClickCountDAO {
    /**
     * 批处理更新用户广告点击量
     * @param adUserClickCounts
     */
    void updateBatch(List<AdUserClickCount> adUserClickCounts);

    /**
     * 根据多个key查询用户广告点击量
     * @param date 日期
     * @param uerid 用户id
     * @param adid 广告id
     * @return
     */
    int findClickCountByMultiKey(String date,long uesrid,long adid);
}
