package dao.impl;

import dao.ITop10CategoryDAO;
import domain.Top10Category;
import jdbc.JDBCHelper;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/20 14:57
 * @Description: top10品类DAO实现
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
