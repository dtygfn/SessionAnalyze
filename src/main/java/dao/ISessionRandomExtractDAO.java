package dao;

import domain.SessionRandomExtract;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:34
 * @Description: session随机抽取模块DAO接口
 */
public interface ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);


}
