package com.fluid.reactive;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Objects;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(classes = ReactiveApplication.class)
@Import({TestChannelBinderConfiguration.class})
@Slf4j
class ReactiveProcessorTest {

    @Autowired
    private InputDestination input;

    @Autowired
    private OutputDestination output;

    @Test
    @SneakyThrows
    void processor() {
        IntStream.range(1, 216).forEach(i -> {
            input.send(
                MessageBuilder.withPayload("start - " + i)
                    .setHeader("sequence_size", 215)
                    .setHeader("sequence_number", i)
                    .setHeader("custom-id", "BAC")
                    .build()
            );
        });

        Thread.sleep(10000);
        Message<byte[]> result = output.receive();
        int count = 0;
        while (Objects.nonNull(result)) {
            final String value = new String(result.getPayload());
            log.info("Value -> {}", value);
            log.info("Headers -> {}", result.getHeaders());

            // verify we did not lose any header in the process
            assertThat(result.getHeaders().get("custom-id")).isEqualTo("BAC");
            assertThat(result.getHeaders().get("sequence_size")).isEqualTo(215);

            count++;
            result = output.receive();
        }

        log.info("Count -> {}", count);
        // verify all message send are processed
        assertThat(count).isEqualTo(215);
    }
}