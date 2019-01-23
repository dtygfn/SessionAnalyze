package dao;

import domain.Task;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 15:35
 * @Description: 任务管理DAO接口
 */
public interface ITaskDAO {
    /**
     * 根据主键查询任务
     * @param taskid 主键id
     * @return 任务
     */
    Task findById(long taskid);
}
