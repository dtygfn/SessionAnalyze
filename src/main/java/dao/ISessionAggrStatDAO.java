package dao;

import domain.SessionAggrStat;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:18
 * @Description: session聚合统计模块DAO接口
 */
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);

}
