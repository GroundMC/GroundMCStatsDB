package com.github.gianttreelp.statsdb;

import com.google.common.collect.Queues;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerStatisticIncrementEvent;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

public class EventListener implements Listener {

    protected Queue<StatisticsObject> statisticsQueue = Queues.newConcurrentLinkedQueue();

    @EventHandler
    public void onStatisticIncrement(PlayerStatisticIncrementEvent event) {
        byte[] uuid = StatsDB.getBytesFromUUID(event.getPlayer());

        Optional<StatisticsObject> statObject = statisticsQueue.stream()
                .filter(stat ->
                        Arrays.equals(stat.uuid, uuid) &&
                                event.getStatistic() != null && Objects.equals(stat.statistic, event.getStatistic().name()) &&
                                event.getMaterial() != null && Objects.equals(stat.material, event.getMaterial().name()) &&
                                event.getEntityType() != null && Objects.equals(stat.entity, event.getEntityType().name()))
                .findFirst();

        if (statObject.isPresent()) {
            statObject.get().value = event.getNewValue();
        } else {
            statisticsQueue.add(new StatisticsObject(
                    uuid,
                    event.getStatistic().name(),
                    event.getMaterial() != null ? event.getMaterial().name() : null,
                    event.getEntityType() != null ? event.getEntityType().name() : null,
                    event.getNewValue()
            ));
        }
    }

    @EventHandler
    public void onJoin(PlayerJoinEvent event) {
        byte[] uuid = StatsDB.getBytesFromUUID(event.getPlayer());
        StatsDB.STATISTIC_LIST.forEach(stat -> statisticsQueue.add(new StatisticsObject(
                uuid,
                stat.name(),
                null,
                null,
                event.getPlayer().getStatistic(stat)
        )));
    }
}
