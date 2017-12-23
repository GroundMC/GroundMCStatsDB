package com.github.gianttreelp.statsdb;

import com.google.common.collect.Queues;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerStatisticIncrementEvent;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class EventListener implements Listener {

    protected Queue<StatisticsObject> statisticsQueue = Queues.newConcurrentLinkedQueue();

    @EventHandler
    public void onStatisticIncrement(PlayerStatisticIncrementEvent event) {
        UUID playerId = event.getPlayer().getUniqueId();
        byte[] uuid = new byte[16];
        ByteBuffer.wrap(uuid).order(ByteOrder.BIG_ENDIAN)
                .putLong(playerId.getMostSignificantBits())
                .putLong(playerId.getLeastSignificantBits());

        Optional<StatisticsObject> statObject = statisticsQueue.stream()
                .filter(stat ->
                        Arrays.equals(stat.uuid, uuid) &&
                                Objects.equals(stat.statistic, event.getStatistic().name()) &&
                                Objects.equals(stat.material, event.getMaterial().name()) &&
                                Objects.equals(stat.entity, event.getEntityType().name()))
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
}
