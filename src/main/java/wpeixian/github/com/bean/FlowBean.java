package wpeixian.github.com.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 1. 实现writable接口
public class FlowBean implements Writable {

    private Long upFlow;    // 上行流量
    private Long downFlow;  // 下行流量
    private long sumFlow;   // 总流量

    // 2. 无参构造
    public FlowBean() {
    }

    // 3. 提供三个参数的getter和setter方法
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

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    // 4. 实现write方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }
}
