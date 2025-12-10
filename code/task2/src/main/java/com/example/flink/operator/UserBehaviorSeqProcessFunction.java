package com.example.flink.operator;

import com.example.flink.model.UserBehavior;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 维护用户最近100次行为序列的KeyedProcessFunction
 */
public class UserBehaviorSeqProcessFunction extends KeyedProcessFunction<String, UserBehavior, String> {

    private transient ValueState<String> userBehaviorSeqState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 状态描述符
        ValueStateDescriptor<String> stateDesc = new ValueStateDescriptor<>(
                "userBehaviorSeqState",
                String.class
        );

        // 设置状态TTL：30分钟（避免状态无限增长）
        org.apache.flink.api.common.state.StateTtlConfig ttlConfig = org.apache.flink.api.common.state.StateTtlConfig
                .newBuilder(Time.minutes(30))
                .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        stateDesc.enableTimeToLive(ttlConfig);

        // 获取状态
        userBehaviorSeqState = getRuntimeContext().getState(stateDesc);
    }

    @Override
    public void processElement(UserBehavior value, Context ctx, Collector<String> out) throws Exception {
        // 获取当前状态（用户行为序列）
        String currentSeq = userBehaviorSeqState.value() == null ? "" : userBehaviorSeqState.value();

        // 拼接新行为，保留最近100次（用逗号分隔）
        String newSeq = currentSeq + "," + value.getBehaviorType();
        String[] behaviors = newSeq.split(",");
        if (behaviors.length > 100) {
            // 截取最近100次行为
            StringBuilder sb = new StringBuilder();
            for (int i = behaviors.length - 100; i < behaviors.length; i++) {
                if (i > behaviors.length - 100) {
                    sb.append(",");
                }
                sb.append(behaviors[i]);
            }
            newSeq = sb.toString();
        }

        // 更新状态
        userBehaviorSeqState.update(newSeq);

        // 输出结果（用于监控，可选）
        out.collect("UserId: " + value.getUserId() + ", BehaviorSeq: " + newSeq);
    }
}