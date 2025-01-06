package com.lartimes.unicom.mapreduce.mr;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 22:19
 */
public class UnicomGroupCleanReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable nullWritable = NullWritable.get();
    private final int length = "yyyyMM".length();
//    final LongAdder count = new LongAdder(); // 不要用atomic 操作

    public UnicomGroupCleanReducer() {
//        redis 文件持久化
//        TODO
        super();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
//        查看length
//        清洗规则
//        进行清洗， 输出文件
//        子job读取数据，划分为12个job，
        LongAdder count = new LongAdder();
        final StringBuilder sb = new StringBuilder();
        final Map<Integer, Object> map = new HashMap<>();
        List<String> parallelList = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(values.iterator(), Spliterator.ORDERED), false)
                .map(Text::toString)
                .sorted((s1, s2) -> {
                    int val1 = Integer.parseInt(s1.substring(0, length));
                    int val2 = Integer.parseInt(s2.substring(0, length));
                    return Integer.compare(val1, val2);
                })
                .toList();

//        System.out.println("Parallel list: " + parallelList);
        for (String row : parallelList) {
            String[] arr = row.replaceAll("\"", "").split(",");
//                TODO 进行数据清理， 再加入子job 两次map reduce , 恢复到原来的结构
            IntStream.range(2, arr.length).forEach(index -> {
                        String value = String.valueOf(map.get(index));
                        switch (index) {
                            case 2 -> {
                                if (arr[2].isBlank()) {
                                    arr[2] = "3G";
                                }
                            }
                            case 3 -> {
                                if (Objects.equals(arr[3], "未知")) {
//                                    整个趋势最多的， 并且此处
                                    arr[3] = value == null ? "男" : value;
                                    return;
                                }
                                if ("null".equals(value)) {
                                    map.put(3, arr[3]);
                                }
                            }
                            case 4 -> {
                                if (!arr[4].isBlank()) {
                                    map.put(4, arr[4]);
                                    return;
                                }
                                if ("null".equals(value)) {
                                    arr[4] = "17以下";
                                }
                            }
                            case 5, 8, 9 -> {
//                                arpu ， 频率最高
                                boolean flag = arr[5].isBlank();
                                Integer dest = index;
                                if (flag || arr[7].isBlank()) {
                                    if (flag) {
                                        dest = 5;
                                    } else {
                                        dest = 8;
                                    }
                                    List<String> list = (List) map.get(dest);
                                    if(list != null){
                                        Map<String, Long> frequencyMap = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
                                        Optional<Map.Entry<String, Long>> maxEntry = frequencyMap.entrySet().stream()
                                                .max(Map.Entry.comparingByValue());
                                        maxEntry.ifPresent(entry -> arr[index] = entry.getKey());
                                        return;
                                    }
                                }
                                List<String> tmp = (List) map.getOrDefault(index, new ArrayList<String>());
                                tmp.add(arr[dest]);
                                map.put(dest, tmp);
                            }
                            case 6, 7 -> {
                                if (arr[6].isBlank() || arr[7].isBlank()) {
                                    if (!"null".equals(value)) {
                                        String[] split = value.split(",");
                                        arr[6] = split[0];
                                        arr[7] = split[1];
                                    }
                                    return;
                                }
                                map.put(6, new StringJoiner(",").add(arr[6]).add(arr[7]));
                            }
                            case 10, 11 -> {
                                if (arr[index].isBlank()) {
                                    int total = Integer.parseInt(value);
                                    arr[index] = String.valueOf(total / count.intValue());
                                    return;
                                }
                                if (NumberUtils.isNumber(arr[index])) {
                                    count.increment();
                                    map.put(index, (Integer) map.getOrDefault(index, 0) + Integer.parseInt(arr[index]));
                                }
                            }
                            default -> {
                                System.err.println("未知错误 : " + row);
                            }
                        }
                    }
            );
            for (String s : arr) {
                sb.append(s).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            outKey.set(sb.toString());
            context.write(outKey, nullWritable);
        }
    }
}

