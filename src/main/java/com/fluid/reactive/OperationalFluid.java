package com.fluid.reactive;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Slf4j
public class OperationalFluid<T, V> {

    private final Integer maxCapacity;
    private final Map<String, DataStore<T, V>> storage = new ConcurrentHashMap<>();

    private final FluidEvent<T> fluidEvent;

    private final Consumer<List<V>> consumer;

    public OperationalFluid(Integer maxCapacity, FluidEvent<T> fluidEvent, Consumer<List<V>> consumer) {
        this.maxCapacity = maxCapacity;
        this.fluidEvent = fluidEvent;
        this.consumer = consumer;
    }


    void add(String id, Pair<T, V> input) {
        if (!storage.containsKey(id)) {
            final DataStore<T, V> store = DataStore.build();
            store.input().add(input.left());
            store.processed().add(input.right());

            storage.put(id, store);
        } else {
            storage.get(id).processed().add(input.right());
            storage.get(id).input().add(input.left());

            if (maxCapacityReached(id)) {
                execute(id);
            }

        }
    }

    private void execute(String id) {
        log.info("Executing... ID -> {}", id);
        DataStore<T, V> data = storage.get(id);
        storage.remove(id);
        try {
            consumer.accept(data.processed());
            fluidEvent.onSuccess(data.input());
        } catch (Exception ex) {
            log.error("", ex);
            fluidEvent.onError(data.input(), ex);
        } finally {
            fluidEvent.onEnd(data.input());
            data.clear();
        }

    }

    public void executeScheduled() {
        storage.keySet().stream().map(String::valueOf).forEach(this::execute);
    }

    private Boolean maxCapacityReached(String id) {
        return storage.get(id).input().size() >= maxCapacity;
    }


    private record DataStore<T, V>(List<T> input, List<V> processed) {
        static <T, V> DataStore<T, V> build() {
            return new DataStore<>(new ArrayList<>(), new ArrayList<>());
        }

        void clear() {
            input.clear();
            processed.clear();
        }
    }
}
