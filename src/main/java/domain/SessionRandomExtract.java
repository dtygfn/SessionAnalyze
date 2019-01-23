package domain;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 10:52
 * @Description: 随机抽取的session
 */
public class SessionRandomExtract {

    private long taskid;        // 任务id
    private String sessionid;   // 会话id
    private String startTime;   // 开始时间
    private String searchKeywords;// 搜索关键字
    private String clickCategoryIds;// 点击的分类id

    public long getTaskid() {
        return taskid;
    }
    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }
    public String getSessionid() {
        return sessionid;
    }
    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }
    public String getStartTime() {
        return startTime;
    }
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }
    public String getSearchKeywords() {
        return searchKeywords;
    }
    public void setSearchKeywords(String searchKeywords) {
        this.searchKeywords = searchKeywords;
    }
    public String getClickCategoryIds() {
        return clickCategoryIds;
    }
    public void setClickCategoryIds(String clickCategoryIds) {
        this.clickCategoryIds = clickCategoryIds;
    }

}
