package com.example.flink;

import com.example.flink.model.ProductSales;
import com.example.flink.model.UserBehavior;
import com.example.flink.model.UserTotalConsume;
import com.example.flink.operator.UserBehaviorSeqProcessFunction;
import com.example.flink.operator.UserTotalConsumeProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Flink检查点容错性实验主作业（无限流版本）
 */
public class CheckpointExperimentJob {

    // 检查点间隔（毫秒）：可通过命令行参数或配置文件修改
    private static final long CHECKPOINT_INTERVAL = 75000; // 默认30秒，可修改为10000/60000/120000

    // 状态规模参数
    private static final int USER_COUNT = 100000;   // 10万用户
    private static final int PRODUCT_COUNT = 50000; // 5万商品

    public static void main(String[] args) throws Exception {
        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置检查点
        configureCheckpoint(env);

        // 3. 生成高吞吐用户行为数据流（无限流）
        DataStream<UserBehavior> behaviorStream = generateUserBehaviorStream(env);

        // 4. 业务逻辑1：用户行为序列状态（ValueState）
        DataStream<String> userBehaviorSeqStream = behaviorStream
                .keyBy(UserBehavior::getUserId)
                .process(new UserBehaviorSeqProcessFunction())
                .setParallelism(4);

        // 5. 业务逻辑2：商品销售额窗口聚合（1分钟滚动窗口）
        DataStream<ProductSales> productSalesStream = behaviorStream
                .filter(behavior -> !behavior.getBehaviorType().equals("click")) // 仅统计下单/支付
                .keyBy(UserBehavior::getProductId)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new ProductSalesProcessWindowFunction())
                .setParallelism(4);

        // 6. 业务逻辑3：用户累计消费状态（ReducingState）
        DataStream<UserTotalConsume> userTotalConsumeStream = behaviorStream
                .filter(behavior -> behavior.getBehaviorType().equals("pay")) // 仅统计支付
                .keyBy(UserBehavior::getUserId)
                .process(new UserTotalConsumeProcessFunction())
                .setParallelism(4);

        // 7. 输出结果（控制台，便于观察）
        userBehaviorSeqStream.print("UserBehaviorSeq").setParallelism(1);
        productSalesStream.print("ProductSales").setParallelism(1);
        userTotalConsumeStream.print("UserTotalConsume").setParallelism(1);

        // 8. 执行作业（无限流，持续运行）
        env.execute("2.job - 75s");
    }

    /**
     * 配置检查点参数
     */
    private static void configureCheckpoint(StreamExecutionEnvironment env) {
        // 启用检查点（Exactly-Once语义）
        env.enableCheckpointing(CHECKPOINT_INTERVAL, CheckpointingMode.EXACTLY_ONCE);

        // 获取检查点配置
        org.apache.flink.streaming.api.environment.CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 检查点超时时间（间隔的2倍）
        checkpointConfig.setCheckpointTimeout(CHECKPOINT_INTERVAL * 2);

        // 最小检查点间隔（间隔的1/2）
        checkpointConfig.setMinPauseBetweenCheckpoints(CHECKPOINT_INTERVAL / 2);

        // 最大并发检查点数
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 检查点存储路径（本地测试用FileSystem，集群用HDFS）
        checkpointConfig.setCheckpointStorage("hdfs://flink-master:9000/checkpoints");

        // 启用外部ized检查点（作业失败后保留检查点）
        checkpointConfig.enableExternalizedCheckpoints(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
    }

    /**
     * 生成高吞吐用户行为数据流（无限流）
     */
    private static DataStream<UserBehavior> generateUserBehaviorStream(StreamExecutionEnvironment env) {
        // 定义数据生成逻辑
        GeneratorFunction<Long, UserBehavior> generatorFunction = new GeneratorFunction<Long, UserBehavior>() {
            @Override
            public UserBehavior map(Long value) throws Exception {
                // 随机生成用户ID（1~USER_COUNT）
                String userId = "user_" + (ThreadLocalRandom.current().nextInt(USER_COUNT) + 1);

                // 随机生成商品ID（1~PRODUCT_COUNT）
                String productId = "prod_" + (ThreadLocalRandom.current().nextInt(PRODUCT_COUNT) + 1);

                // 随机生成行为类型：click(60%), order(30%), pay(10%)
                String[] behaviors = {"click", "order", "pay"};
                double random = ThreadLocalRandom.current().nextDouble();
                String behaviorType;
                if (random < 0.6) {
                    behaviorType = behaviors[0];
                } else if (random < 0.9) {
                    behaviorType = behaviors[1];
                } else {
                    behaviorType = behaviors[2];
                }

                // 行为时间戳
                long timestamp = System.currentTimeMillis();

                // 金额：点击为0，下单/支付为10~1000的随机数
                double amount = behaviorType.equals("click") ? 0 : ThreadLocalRandom.current().nextDouble(10, 1000);

                return new UserBehavior(userId, productId, behaviorType, timestamp, amount);
            }
        };

        // 创建DataGeneratorSource（Flink 1.17兼容版本）
        // 使用Long.MAX_VALUE作为生成的数据量，实现无限流
        DataGeneratorSource<UserBehavior> dataSource = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE, // 无限流：生成的数据总量
                Types.POJO(UserBehavior.class) // 数据类型
        );

        // 生成数据流并设置水印
        return env.fromSource(
                dataSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), // 允许5秒乱序
                "HighThroughputUserBehaviorSource"
        ).setParallelism(4);
    }

    /**
     * 商品销售额窗口处理函数
     */
    private static class ProductSalesProcessWindowFunction extends ProcessWindowFunction<
            UserBehavior, ProductSales, String, TimeWindow> {

        @Override
        public void process(String productId, Context context, Iterable<UserBehavior> elements, Collector<ProductSales> out) throws Exception {
            // 统计订单数和总销售额
            long orderCount = 0;
            double totalSales = 0;

            for (UserBehavior behavior : elements) {
                orderCount++;
                totalSales += behavior.getAmount();
            }

            // 输出结果，包含并行子任务ID（用于定位故障节点）
            out.collect(new ProductSales(
                    Thread.currentThread().getName(),
                    orderCount,
                    totalSales,
                    context.window().getEnd()
            ));
        }
    }
}