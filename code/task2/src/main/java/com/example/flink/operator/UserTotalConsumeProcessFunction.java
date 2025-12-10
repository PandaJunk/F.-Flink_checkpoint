package com.example.flink.operator;

import com.example.flink.model.UserBehavior;
import com.example.flink.model.UserTotalConsume;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 维护用户累计消费金额的KeyedProcessFunction
 */
public class UserTotalConsumeProcessFunction extends KeyedProcessFunction<String, UserBehavior, UserTotalConsume> {

    private transient ReducingState<Double> userTotalConsumeState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 状态描述符：累加支付金额
        ReducingStateDescriptor<Double> stateDesc = new ReducingStateDescriptor<>(
                "userTotalConsumeState",
                Double::sum,  // 累加器函数
                Double.class
        );

        // 获取状态
        userTotalConsumeState = getRuntimeContext().getReducingState(stateDesc);
    }

    @Override
    public void processElement(UserBehavior value, Context ctx, Collector<UserTotalConsume> out) throws Exception {
        // 累加支付金额到状态
        userTotalConsumeState.add(value.getAmount());

        // 修复：直接获取当前状态值，ReducingState存储的是单个累加值
        double totalAmount = 0.0;
        try {
            // 尝试获取当前状态值，如果状态为空会抛出异常
            totalAmount = userTotalConsumeState.get();
        } catch (Exception e) {
            // 状态为空时，设置初始值
            totalAmount = 0.0;
        }

        // 输出结果
        out.collect(new UserTotalConsume(
                value.getUserId(),
                totalAmount,
                System.currentTimeMillis()
        ));
    }
}