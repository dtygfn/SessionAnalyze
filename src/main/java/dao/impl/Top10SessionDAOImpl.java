package dao.impl;

import dao.ITop10SessionDAO;
import domain.Top10Session;
import jdbc.JDBCHelper;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:43
 * @Description: top10活跃session的DAO实现
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_category_session values(?,?,?,?)";

        Object[] params = new Object[]{top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
