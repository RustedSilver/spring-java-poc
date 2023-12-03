package com.fluid.reactive;

import org.springframework.util.Assert;

public record Pair<T, V>(T left, V right) {
    public Pair {
        Assert.notNull(left, "Pair left value can not be null");
        Assert.notNull(right, "Pair right value can not be null");
    }
}
