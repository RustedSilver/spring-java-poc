package com.fluid.reactive;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Slf4j
@Component
@EnableScheduling
public class ReactiveProcessor {
    private final OperationalFluid<Message<String>, String> fluid;

    @Autowired
    public ReactiveProcessor(FluidEventHandler handler) {
        this.fluid = new OperationalFluid<>(10, handler, s -> {
            s.stream().map(String::toUpperCase).forEach(l -> {
                log.info("Processed -> {}", l);
            });
        });

    }

    @Bean
    public Consumer<Message<String>> listener() {
        return input -> {
            log.info("{}", input);
            final String val = input.getPayload().replace("start", "modified");
            fluid.add("id-1", new Pair<>(input, val));
        };
    }

    @Scheduled(fixedRate = 1000L)
    void execute() {
        log.info("Execution with schedule");
        fluid.executeScheduled();
    }
}
