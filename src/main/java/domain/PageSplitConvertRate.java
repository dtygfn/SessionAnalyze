package domain;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 10:36
 * @Description: 页面切片转换率
 */
public class PageSplitConvertRate {
    private long taskid;
    private String convertRate;

    public long getTaskid() {
        return taskid;
    }
    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }
    public String getConvertRate() {
        return convertRate;
    }
    public void setConvertRate(String convertRate) {
        this.convertRate = convertRate;
    }
}
