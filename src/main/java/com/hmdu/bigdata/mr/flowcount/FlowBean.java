package com.hmdu.bigdata.mr.flowcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:30 2018/7/2
 * @ writable可以将对象序列化
 */
public class FlowBean implements Writable {
    // 设置属性
    private String phoneNum;
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public FlowBean(String phoneNum, Long upFlow, Long downFlow) {
        this.phoneNum = phoneNum;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    // 如果无参构造函数被覆盖，一定要显示的定义，否则在反序列化的时候，会发生异常
    public FlowBean() {
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "phoneNum='" + phoneNum + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    // 序列化，将对象的属性值转换成序列化的值
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.phoneNum);
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);
    }

    // 反序列化，将序列化的值转换成普通数据类型并传入属性
    @Override
    public void readFields(DataInput in) throws IOException {
        phoneNum = in.readUTF();
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
}
