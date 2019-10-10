package com.insolence.pawnshop.report.util;

import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class Pair<L, R> {
    private final R right;
    private final L left;

    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }
}
