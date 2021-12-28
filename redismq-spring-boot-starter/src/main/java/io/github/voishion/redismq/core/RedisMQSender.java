package io.github.voishion.redismq.core;

import cn.hutool.core.util.StrUtil;
import io.github.voishion.redismq.RedisMQMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.Serializable;
import java.util.Objects;

/**
 * Redis队列消息发送器
 *
 * @author lilu
 */
@Slf4j
public class RedisMQSender {

    private final RedisTemplate redisTemplate;

    public RedisMQSender(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void send(String queueName, Serializable message) {
        if (StrUtil.isBlank(queueName) || Objects.isNull(message)) {
            return;
        }
        RedisMQMessage redisMQMessage = new RedisMQMessage();
        redisMQMessage.setQueueName(queueName);
        redisMQMessage.setCreateTime(System.currentTimeMillis());
        redisMQMessage.setPayload(message);

        Long length = redisTemplate.opsForList().leftPush(queueName, redisMQMessage);
        log.info("发送Redis队列消息，发送后队列长度:{}，队列名称:{}，消息内容:{}", length, queueName, redisMQMessage);
    }

}
