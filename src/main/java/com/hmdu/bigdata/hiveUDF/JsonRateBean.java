package com.hmdu.bigdata.hiveUDF;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 16:00 2018/7/7
 * @
 */
public class JsonRateBean {
    private String movie;
    private String rate;
    private String timeStamp;
    private String uid;

    public String getMovie() {
        return movie;
    }

    public String getRate() {
        return rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public JsonRateBean() {
    }

    public JsonRateBean(String movie, String rate, String timeStamp, String uid) {

        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }

    @Override
    public String toString() {
        return movie + "\t" + rate + "\t" + timeStamp + "\t" + uid;
    }
}
