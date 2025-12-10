import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 大状态流作业：模拟大量key的窗口聚合，用于检查点机制分析
 */
public class FlinkCheckpointAnalysis {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ==================== 1. 检查点配置（核心：可通过参数动态调整间隔） ====================
        // 从命令行参数获取检查点间隔（单位：秒），默认30s
        int checkpointInterval = args.length > 0 ? Integer.parseInt(args[0]) : 210;
        // 开启检查点
        env.enableCheckpointing(checkpointInterval * 1000L); // 检查点间隔（毫秒）
        // 高级配置（根据需要调整）
        env.getCheckpointConfig().setCheckpointTimeout(60000L); // 检查点超时时间（60s）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L); // 检查点最小间隔（5s，避免检查点重叠）
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 最大并发检查点数（1，串行执行）
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // 容忍的检查点失败次数

        // ==================== 2. 作业并行度配置 ====================
        env.setParallelism(4); // 作业并行度（根据集群资源调整）

        // ==================== 3. 数据源：生成大量key的流数据 ====================
        DataStream<Tuple3<String, Long, Double>> sourceStream = env.addSource(new BigStateSource())
                .name("Big-State-Source")
                .setParallelism(4); // Source并行度

        // ==================== 4. Keyed操作：按key分组，生成大状态 ====================
        DataStream<String> resultStream = sourceStream
                // 按key分组（key数量由source控制，这里模拟10万+key）
                .keyBy(t -> t.f0)
                // 10分钟滚动窗口（窗口越大，状态越大）
                .window(TumblingProcessingTimeWindows.of(Time.minutes(60)))
                // 聚合操作：计算每个窗口内的平均值
                .aggregate(new AverageAggregate())
                .name("Window-Aggregation")
                .setParallelism(4); // 聚合算子并行度

        // 输出结果（生产环境建议替换为Kafka/HDFS等，避免IO瓶颈）
        resultStream.print().setParallelism(4);

        env.execute("Checkpoint Analysis Job (Interval: " + checkpointInterval + "s)");
    }

    /**
     * 数据源：生成大量key的流数据（模拟大状态场景）
     */
    public static class BigStateSource extends RichParallelSourceFunction<Tuple3<String, Long, Double>> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final int KEY_COUNT = 1000000; // 模拟10万个key（可调整）

        @Override
        public void run(SourceContext<Tuple3<String, Long, Double>> ctx) throws Exception {
            while (running) {
                // 随机生成key（1~10万）
                String key = "key-" + (random.nextInt(KEY_COUNT) + 1);
                // 随机生成value
                double value = random.nextDouble() * 1000;
                // 发送数据（当前时间戳）
                ctx.collect(Tuple3.of(key, System.currentTimeMillis(), value));
                // 控制发送速率（每秒约1万条，可调整）
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 聚合函数：计算窗口内的平均值
     */
    public static class AverageAggregate implements AggregateFunction<
            Tuple3<String, Long, Double>, // 输入类型
            Tuple3<Long, Double, Integer>, // 累加器类型（count, sum, windowSize）
            String> { // 输出类型

        @Override
        public Tuple3<Long, Double, Integer> createAccumulator() {
            return Tuple3.of(0L, 0.0, 0);
        }

        @Override
        public Tuple3<Long, Double, Integer> add(Tuple3<String, Long, Double> value, Tuple3<Long, Double, Integer> accumulator) {
            return Tuple3.of(
                    accumulator.f0 + 1, // 计数+1
                    accumulator.f1 + value.f2, // 总和累加
                    accumulator.f2 + 1 // 窗口内数据量
            );
        }

        @Override
        public String getResult(Tuple3<Long, Double, Integer> accumulator) {
            double avg = accumulator.f1 / accumulator.f0;
            return "Key: " + Thread.currentThread().getName() + 
                   " | Count: " + accumulator.f0 + 
                   " | Avg: " + String.format("%.2f", avg);
        }

        @Override
        public Tuple3<Long, Double, Integer> merge(Tuple3<Long, Double, Integer> a, Tuple3<Long, Double, Integer> b) {
            return Tuple3.of(
                    a.f0 + b.f0,
                    a.f1 + b.f1,
                    a.f2 + b.f2
            );
        }
    }
}