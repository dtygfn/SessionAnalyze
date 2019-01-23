package dao;

import domain.AdClickTrend;

import java.util.List;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:08
 * @Description: 广告点击趋势DAO接口
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
