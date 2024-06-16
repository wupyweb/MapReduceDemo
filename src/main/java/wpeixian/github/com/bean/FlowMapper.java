package wpeixian.github.com.bean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text OutK = new Text();
    private FlowBean OutV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split("\t");

        OutK.set(parts[1]);
        OutV.setUpFlow(Long.parseLong(parts[3]));
        OutV.setDownFlow(Long.parseLong(parts[4]));
        OutV.setSumFlow();

        context.write(OutK, OutV);
    }
}
