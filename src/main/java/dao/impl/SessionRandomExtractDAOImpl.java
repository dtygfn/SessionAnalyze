package dao.impl;

import dao.ISessionRandomExtractDAO;
import domain.SessionRandomExtract;
import jdbc.JDBCHelper;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/20 15:00
 * @Description: 随机抽取session的DAO实现
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
