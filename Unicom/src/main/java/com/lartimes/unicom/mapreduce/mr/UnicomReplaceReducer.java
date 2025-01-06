package com.lartimes.unicom.mapreduce.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/6 12:34
 */
public class UnicomReplaceReducer extends Reducer<Text, Text, Text, NullWritable> {

//    进行groupBy / imsi
//    滑动窗口 添加字段
//     py 处理

    private final int length = "yyyyMM".length();
    private final Text outKey = new Text();
    private final NullWritable nullWritable = NullWritable.get();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        List<String> parallelList = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(values.iterator(), Spliterator.ORDERED), false)
                .map(Text::toString)
                .sorted((s1, s2) -> {
                    int val1 = Integer.parseInt(s1.substring(0, length));
                    int val2 = Integer.parseInt(s2.substring(0, length));
                    return Integer.compare(val1, val2);
                })
                .toList();
        int left = 0;
        int right = 0;
        int length = parallelList.size();
        String str = parallelList.get(left);
        String model = null;
//201501,588c1c909a42df092fd9f6b0793091fe,2G,男,6,1,LG,LG-P503,1,0,0
        String newModel = null;
        outKey.set((str + ",0"));
        context.write(outKey , nullWritable);
        while (right < length -1) {
            right ++ ;
            String s = parallelList.get(right);
            model = str.split(",")[7];
            newModel = s.split(",")[7];
            if(!model.equals(newModel)) {
                outKey.set(s + ",1"); //换手机
                context.write(outKey , nullWritable);
                left = right;
                str = parallelList.get(left);
            }else {
                outKey.set(s + ",0");
                context.write(outKey , nullWritable);
            }
        }
    }
}
