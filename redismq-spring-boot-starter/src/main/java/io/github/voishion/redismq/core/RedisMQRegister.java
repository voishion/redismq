package io.github.voishion.redismq.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import io.github.voishion.redismq.RedisMQListenerTarget;
import io.github.voishion.redismq.RedisMQMessage;
import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author lilu
 */
@Slf4j
public class RedisMQRegister implements ApplicationRunner, ApplicationContextAware, DisposableBean {

    private static final String THREAD_PREFIX = "redis-mq-thread-";

    private final Set<String> registerQueueName = new HashSet<>();

    private final List<Worker> registerQueueThreads = new ArrayList<>();

    private final RedisTemplate redisTemplate;

    private ApplicationContext applicationContext;

    public RedisMQRegister(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws Exception {
        if (CollUtil.isNotEmpty(registerQueueThreads)) {
            registerQueueThreads.forEach(worker -> worker.exit());
        }
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        initializer();
        for (String queueName : registerQueueName) {
            Worker worker = new Worker(queueName);
            worker.setName(THREAD_PREFIX + queueName);
            registerQueueThreads.add(worker);
            worker.start();
            log.info("??????????????????????????????{}???", queueName);
        }
    }

    private void initializer() {
        List<RedisMQListenerTarget> redisMQListenerTargets = RedisMQListenerScanner.getRedisListenerTargets();
        if (CollUtil.isNotEmpty(redisMQListenerTargets)) {
            for (RedisMQListenerTarget redisMQListenerTarget : redisMQListenerTargets) {
                registerQueueName.add(redisMQListenerTarget.getQueueName());
            }
        }
    }

    private class Worker extends Thread {

        private boolean exit = false;

        private final String queueName;

        private final String queueNameBak;

        private List<RedisMQListenerTarget> consumerTargets = new ArrayList<>();

        /**
         * ???????????????????????????????????????${spring.redis.timeout}??????????????????????????????????????????????????????
         * 10???
         */
        private int TIME_OUT = 10;

        private Worker(String queueName) {
            this.queueName = queueName;
            this.queueNameBak = queueName + ":bak";
            initConsumerTargets();
            checkNeedRecoverMessage();
        }

        private void initConsumerTargets() {
            if (consumerTargets.size() == 0) {
                List<RedisMQListenerTarget> redisMQListenerTargets = RedisMQListenerScanner.getRedisListenerTargets();
                consumerTargets.addAll(redisMQListenerTargets.stream().filter(f -> f.match(queueName)).collect(Collectors.toList()));
            }
        }

        @Override
        public void run() {
            if (StrUtil.isBlank(queueName)) {
                return;
            }
            while (!exit) {
                try {
                    // ????????????????????????key?????????????????????????????????????????????????????????????????????????????????????????????
                    RedisMQMessage message = (RedisMQMessage) redisTemplate.opsForList().rightPopAndLeftPush(queueName, queueNameBak, TIME_OUT, TimeUnit.SECONDS);
                    if (exit) {
                        recoverMessage();
                        log.error("???????????????{}??????????????????????????????", queueName);
                    } else {
                        if (Objects.nonNull(message)) {
                            handleMessage(message);
                            clearBakMessage();
                        }
                    }
                } catch (IllegalAccessException | InvocationTargetException e) {
                    log.error("???????????????{}????????????????????????, error=>{}", queueName, Throwables.getStackTraceAsString(e));
                    recoverMessage();
                }
            }
        }

        private void handleMessage(RedisMQMessage message) throws IllegalAccessException, InvocationTargetException {
            if (Objects.nonNull(message) && consumerTargets.size() > 0) {
                for (RedisMQListenerTarget target : consumerTargets) {
                    Method targetMethod = target.getMethod();
                    if (target.getMethodParameterClassName().equals(RedisMQMessage.class.getName())) {
                        targetMethod.invoke(target.getBean(applicationContext), message);
                    } else if (target.getMethodParameterClassName().equalsIgnoreCase(message.getPayload().getClass().getName())) {
                        targetMethod.invoke(target.getBean(applicationContext), message.getPayload());
                    } else {
                        throw new RedisMQException(StrUtil.format("???????????????{}???????????????????????????{}??????????????????????????????????????????", queueName, targetMethod.getName()));
                    }
                }
            }
        }

        private void checkNeedRecoverMessage() {
            while (redisTemplate.opsForList().size(queueNameBak) > 0) {
               recoverMessage();
            }
        }

        private void recoverMessage() {
            redisTemplate.opsForList().leftPush(queueName, redisTemplate.opsForList().leftPop(queueNameBak));
        }

        private void clearBakMessage() {
            redisTemplate.opsForList().leftPop(queueNameBak);
        }

        public void exit() {
            this.exit = true;
            log.error("???????????????{}???????????????", queueName);
        }

    }

}
