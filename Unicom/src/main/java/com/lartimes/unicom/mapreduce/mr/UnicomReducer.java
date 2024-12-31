package com.lartimes.unicom.mapreduce.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 22:19
 */
public class UnicomReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable nullWritable = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
//        查看length
//        清洗规则
//        进行清洗， 输出文件
//        子job读取数据，划分为12个job，
        List<String> parallelList = StreamSupport.stream(values.spliterator(), true)
                .map(Text::toString)
                .sorted((s1 ,s2) -> {
                    int val1 = Integer.parseInt(s1.substring(0, "yyyyMM".length()));
                    int val2 = Integer.parseInt(s2.substring(0, "yyyyMM".length()));
                    return Integer.compare(val2, val1);
                })
                .toList();
        System.out.println("Parallel list: " + parallelList);
//        net 之前出现即可
//        sex 不是未知 即可，
//        age_weight 找出第一个不为null ， 否则的话,redis提供平均 / 默认值即可
//        arpu ， 频率最高
//        brand 之前用的品牌
//        之前品牌的机型 ， 否的的话 ， 到redis去查 / null ， 数据库后台更新
//        流量 之前的平均值
//        callsum 之前的平均值
//        sms——total 之前的平均值
            parallelList.forEach(row -> {
                String[] arr = row.split(",");
//                TODO 进行数据清理， 再加入子job 两次map reduce , 恢复到原来的结构
//                后面就是分析存储的架构了
//                还有后面的模型变化跟预测
//                201501,d8ccc2441daabc76628b8ce9ffc9446e,3G,男,30-39,50-99,Xiaomi,MI 2013029,0-499,377,0
                IntStream.range(2 , arr.length + 1).forEach( index -> {
                            switch (index){
                                case 2-> {
                                    if( arr[2].isBlank()){
                                        arr[2] = "3G";
                                    }
                                }
                                case 3 -> if(Objects.equals(arr[3], "未知")
                                        map.get(3).var
                                )
                                case 4 -> System.out.println("为双精度浮点数：" + doubleR);
                                case 5 -> System.out.println("为字符串：" + str);
                                case 6-> System.out.println("为字符串：" + str);
                                case 7 -> System.out.println("为字符串：" + str);
                                case 8 -> System.out.println("为字符串：" + str);
                                case 9 -> System.out.println("为字符串：" + str);
                                case 10 -> System.out.println("为字符串：" + str);
                                default -> System.out.println("其他类型：" + obj);
                            }
                        }
                 );
            });
//
    }
    private Map<Integer , Object > map = new ConcurrentHashMap<Integer , Object>();


}
