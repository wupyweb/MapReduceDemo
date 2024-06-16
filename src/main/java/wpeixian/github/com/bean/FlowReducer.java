package wpeixian.github.com.bean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean OutV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        Long upTotal = 0L;
        Long downTotal = 0L;

        for (FlowBean value : values) {
            upTotal += OutV.getUpFlow();
            downTotal += OutV.getDownFlow();
        }
        OutV.setUpFlow(upTotal);
        OutV.setDownFlow(downTotal);
        OutV.setSumFlow();

        context.write(key, OutV);
    }
}
