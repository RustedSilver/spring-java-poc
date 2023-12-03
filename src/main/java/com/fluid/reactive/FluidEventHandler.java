package com.fluid.reactive;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamOperations;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class FluidEventHandler implements FluidEvent<Message<String>> {

    private final StreamOperations operations;

    @Autowired
    public FluidEventHandler(StreamOperations operations) {
        this.operations = operations;
    }

    @Override
    public void onSuccess(List<Message<String>> data) {
        data.forEach(d -> {
            log.info("V: {}", d);
        });
        log.info("Success -> {}", data);
    }

    @Override
    public void onError(List<Message<String>> data, Exception ex) {
        data.forEach(d -> {
            log.info("Error: {}", d);
        });
        log.info("Error -> {}", ex.getMessage());
    }

    @Override
    public void onEnd(List<Message<String>> data) {
        log.info("Send to output -> ");
        data.forEach(d -> {
            operations.send("listener-out-0", MessageBuilder.withPayload(d).copyHeaders(d.getHeaders()).build());
        });
    }
}
