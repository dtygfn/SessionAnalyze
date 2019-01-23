package domain;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 10:30
 * @Description: 用户点击量
 */
public class AdUserClickCount {
    private String date; // 统计时间
    private long userid; // 用户id
    private long adid;  // 广告id
    private long clickCount;  // 点击次数

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public long getAdid() {
        return adid;
    }

    public void setAdid(long adid) {
        this.adid = adid;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
