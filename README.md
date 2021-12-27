# RedisMQ

> [参考对象](https://gitee.com/jo8tony/redis-mq) ，在其基础上加入了可靠消息、销毁不消费等逻辑

- ## Redis消息队列

[Redis 消息队列的三种方案（List、Streams、Pub/Sub）](https://zhuanlan.zhihu.com/p/344269737)

[Redis实现消息队列的4种方案](https://www.jianshu.com/p/d32b16f12f09)

- ### RedisMQ功能使用说明

  该模块是使用基于Redis列表数据类型实现的具有**消息队列、可靠消息**功能的模块，该项目建立在Spring Boot框架之上，通过Spring提供的`RedisTemplate`功能访问Redis服务。

- ###  如何使用

1. 添加配置

   ```yaml
   spring:
     redis:
       message-queue:
         producer: true # 是否开启RedisMQ生产者模式
         consumer: true # 是否开启RedisMQ消费者模式
    ```
  
2. 消息生产者示例
  
     ```java
     @Log4j2
     @RestController
     @RequiredArgsConstructor
     public class RedisMQController {
     
         private final RedisMQSender redisMQSender;
     
         @GetMapping("hello3")
         public String send3() {
             for (int i = 0; i < 50000; i++) {
                 if (i % 2 == 0) {
                     redisMQSender.send(RedisMQConstant.TEST_QUEUE, String.valueOf(i));
                 } else {
                     redisMQSender.send(RedisMQConstant.TEST_QUEUE_2, String.valueOf(i));
                 }
                 if (i == 25000) {
                     System.out.println(i);
                 }
             }
             return "";
         }
     
     }
   ```
  
3. 消息消费者示例
  
   > 创建一个`RedisMQListenerContainer`类用于定义redis队列消息监听处理方法。 实现redis队列监听只需在Spring容器所管理的Bean中的方法上添加注解`@RedisMQListener(队列名称)`，队列名称是一个String类型的并且不能为空，表示该方法你需要处理的哪个队列的消息。注意被`@RedisMQListener`修饰的方法只能包含一个参数，这个参数可以是一个实现了`java.io.Serializable`接口的实体类或者包装类型参数，也可以是一个`RedisMQMessage`泛型类。
  
     ```java
     @Log4j2
     @Component
     public class RedisMQListenerContainer {
     
         @RedisMQListener(RedisMQConstant.TEST_QUEUE)
         public void dealRedisMessage0(RedisMQMessage message) {
             log.info("dealRedisMessage0收到queue-1队列消息: {}", message);
         }
     
         @RedisMQListener(RedisMQConstant.TEST_QUEUE)
         public void dealRedisMessage1(String message) {
             log.info("dealRedisMessage0收到queue-1队列消息: {}", message);
         }
     
         @RedisMQListener(RedisMQConstant.TEST_QUEUE_2)
         public void dealRedisMessage2(RedisMQMessage<String> message) {
             log.info("dealRedisMessage0收到queue-2队列消息: {}", message);
         }
     
     }
   ```
  
     **注意**：你可以定义多个带有`@RedisMQListener(队列名称)`注解的方法，并且队列名称相同，注意如果这样该队列中的同一消息会被这些方法重复消费。分布式集群环境中不同服务监听同一队列，同一条消息只会被其中一个服务上的所有监听该队列的方法消费。