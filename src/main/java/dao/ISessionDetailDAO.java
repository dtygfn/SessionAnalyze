package dao;

import domain.SessionDetail;

import java.util.List;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:24
 * @Description: Session明细DAO接口
 */
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);

    /**
     * 批量插入session明细数据
     * @param sessionDetails
     */
    void insertBatch(List<SessionDetail> sessionDetails);

}
