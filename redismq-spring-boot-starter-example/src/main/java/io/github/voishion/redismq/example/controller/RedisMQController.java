package io.github.voishion.redismq.example.controller;

import io.github.voishion.redismq.core.RedisMQSender;
import io.github.voishion.redismq.example.constant.RedisMQConstant;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lilu
 */
@Log4j2
@RestController
@RequiredArgsConstructor
public class RedisMQController {

    private final RedisMQSender redisMQSender;

    @GetMapping("send1/{message}")
    public String send1(@PathVariable String message) {
        redisMQSender.send(RedisMQConstant.TEST_QUEUE, message);
        return "1";
    }

    @GetMapping("send2/{message}")
    public String send2(@PathVariable String message) {
        redisMQSender.send(RedisMQConstant.TEST_QUEUE_2, message);
        return "1";
    }

    @GetMapping("send3")
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

    @GetMapping("send4")
    public String send4() {
        for (int i = 0; i < 10; i++) {
            redisMQSender.send(RedisMQConstant.TEST_QUEUE_2, String.valueOf(i));
        }
        return "";
    }

    @GetMapping("send5")
    public String send5() {
        for (int i = 0; i < 100000; i++) {
            redisMQSender.send(RedisMQConstant.TEST_QUEUE_2, String.valueOf(i));
        }
        return "";
    }

}
