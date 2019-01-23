package dao;

import domain.AreaTop3Product;

import java.util.List;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:16
 * @Description: 各区域top3热门商品DAO接口
 */
public interface IAreaTop3ProductDAO {
    /**
     * 使用批处理的方式向area_top3_product表插入数据
     * @param areaTopsProducts
     */
    void insertBatch(List<AreaTop3Product> areaTopsProducts);

}
