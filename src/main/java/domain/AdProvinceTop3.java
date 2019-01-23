package domain;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 10:16
 * @Description: 各省top3热门广告
 */
public class AdProvinceTop3 {
    private String date;    // 统计时间
    private String province; // 省市
    private long adid;      // 广告id
    private long clickCount;// 点击次数

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
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
