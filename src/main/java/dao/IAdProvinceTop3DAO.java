package dao;

import domain.AdProvinceTop3;

import java.util.List;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:10
 * @Description: 各省市top3热门广告DAO接口
 */
public interface IAdProvinceTop3DAO {
    void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
