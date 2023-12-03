package com.fluid.reactive;

import org.springframework.messaging.MessageHeaders;

import java.util.List;

public interface FluidEvent<T> {
    void onSuccess(List<T> data);

    void onError(List<T> data, Exception ex);

    void onEnd(List<T> data);
}
