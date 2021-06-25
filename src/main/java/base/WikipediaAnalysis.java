package base;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;


public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {
        //创建一个streaming程序运行的上下文
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //source 数据来源
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        //transform
        //DataStream -> KeyedStream
        KeySelector<WikipediaEditEvent, String> keySelector = new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent event) throws Exception {
                return event.getUser();
            }
        };
        KeyedStream<WikipediaEditEvent, String> keyedStream = edits
                .keyBy(keySelector); //以返回每一条修改事件的user为key 分区
        /*lambda .keyBy(event -> event.getUser())
        * .keyBy(WikipediaEditEvent::getUser）*/

        //KeyedStream -> DataStream
        DataStream<Tuple2<String, Long>> result = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
                        return new Tuple2<>(value.getUser(), value.getByteDiff() + accumulator.f1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

        result.map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> value) throws Exception {
                return value.toString();
            }
        }).addSink(new FlinkKafkaProducer<String>("localhost:9092", "wiki", new SimpleStringSchema()));
        //发送到kafka或者其它存储; 打印的 前缀 > 代表第几个subtask

        see.execute();
    }
}
