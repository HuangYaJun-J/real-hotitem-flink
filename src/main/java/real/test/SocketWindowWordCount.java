package real.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @description: SocketWindowWordCount
 * @author: HuangYaJun
 * @Email: huangyajun_j@163.com
 * @create: 2019/1/19 10:10
 */
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //在本机执行 nc -lk 9001
        DataStreamSource<String> text = env.socketTextStream("newdata", 8001, "\n");

        DataStream<Tuple2<String, Integer>> wordCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split("\\s")) {//对空白符进行切割, 比如空格,换行符, 制表符等
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        DataStream<Tuple2<String, Integer>> windowCounts = wordCounts.keyBy(0)
                .timeWindow(Time.seconds(1))
                .sum(1);

        windowCounts.print().setParallelism(1);
        env.execute("=====Socket Window WordCount======");
    }
}
