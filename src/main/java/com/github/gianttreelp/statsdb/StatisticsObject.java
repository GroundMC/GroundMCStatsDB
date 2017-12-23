package com.github.gianttreelp.statsdb;

public final class StatisticsObject {
    int value;
    final byte[] uuid;
    final String statistic;
    final String material;
    final String entity;

    StatisticsObject(final byte[] uuid, final String statistic, final String material, final String entity, final int value) {
        this.uuid = uuid;
        this.statistic = statistic;
        this.material = material;
        this.entity = entity;
        this.value = value;
    }
}
