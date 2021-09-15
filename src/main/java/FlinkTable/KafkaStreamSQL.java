package FlinkTable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Properties;

public class KafkaStreamSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("corn", new SimpleStringSchema(), properties);
        //从起始位置进行消费
        consumer.setStartFromEarliest();

        DataStreamSource<String> topic = env.addSource(consumer);

        SingleOutputStreamOperator<String> kafkaSource = topic.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        //注册内存表
        tEnv.registerDataStream("book", kafkaSource, "name");
        //sql
        Table result = tEnv.sqlQuery("select name, count(1) from book group by name");

        //回退更新
        tEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}
