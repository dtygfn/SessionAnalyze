package dao.impl;

import dao.IPageSplitConvertRateDAO;
import domain.PageSplitConvertRate;
import jdbc.JDBCHelper;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/20 14:30
 * @Description: 页面切片转化率DAO实现类
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
