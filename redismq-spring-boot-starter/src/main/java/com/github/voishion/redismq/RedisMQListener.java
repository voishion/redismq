package com.github.voishion.redismq;

import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * Redis队列监听注解
 *
 * @author lilu
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisMQListener {

    @AliasFor("queueName")
    String value() default "";

    @AliasFor("value")
    String queueName() default "";

}
