package com.github.gianttreelp.statsdb;

import org.bukkit.Statistic;

final class StatisticsObject {
    final byte[] uuid;
    final Statistic statistic;
    final String material;
    final String entity;
    int value;

    StatisticsObject(final byte[] uuid, final Statistic statistic,
                     final String material, final String entity,
                     final int value) {
        this.uuid = uuid;
        this.statistic = statistic;
        this.material = material;
        this.entity = entity;
        this.value = value;
    }
}
