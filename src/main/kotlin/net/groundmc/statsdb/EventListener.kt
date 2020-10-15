package net.groundmc.statsdb

import com.google.common.collect.Queues
import kotlinx.coroutines.launch
import org.bukkit.Statistic
import org.bukkit.event.EventHandler
import org.bukkit.event.Listener
import org.bukkit.event.player.PlayerJoinEvent
import org.bukkit.event.player.PlayerQuitEvent
import org.bukkit.event.player.PlayerStatisticIncrementEvent
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.experimental.suspendedTransactionAsync
import java.util.*

internal class EventListener(private val statsDb: StatsDB) : Listener {

    val statisticsQueue: Queue<StatisticsObject> = Queues.newConcurrentLinkedQueue<StatisticsObject>()

    @EventHandler
    fun onStatisticIncrement(event: PlayerStatisticIncrementEvent) {
        val uuid = event.player.uniqueId

        val statObject = statisticsQueue
                .firstOrNull { (uuid1, statistic, material, entity) ->
                    uuid1 == uuid &&
                            statistic == event.statistic &&
                            event.material == material &&
                            event.entityType == entity
                }

        if (statObject != null) {
            statObject.value = event.newValue
        } else {
            statisticsQueue.add(StatisticsObject(
                    uuid,
                    event.statistic,
                    event.material,
                    event.entityType,
                    event.newValue
            ))
        }
    }

    @EventHandler
    fun onJoin(event: PlayerJoinEvent) {
        statsDb.scope.launch {
            suspendedTransactionAsync(db = statsDb.database) {
                statsDb.statistics.select {
                    statsDb.statistics.uuid eq event.player.uniqueId
                }.map {
                    StatisticsObject(
                            it[statsDb.statistics.uuid],
                            it[statsDb.statistics.statistic],
                            it[statsDb.statistics.material],
                            it[statsDb.statistics.entity],
                            it[statsDb.statistics.value]
                    )
                }
            }.await().forEach {
                when (it.statistic.type) {
                    Statistic.Type.UNTYPED -> event.player.setStatistic(it.statistic, it.value)
                    Statistic.Type.ENTITY -> event.player.setStatistic(it.statistic, it.entity, it.value)
                    Statistic.Type.BLOCK, Statistic.Type.ITEM -> event.player.setStatistic(it.statistic, it.material, it.value)
                    else -> {
                    }
                }
            }
            StatsDB.UPDATE_STATISTICS.forEach { stat ->
                statisticsQueue.add(StatisticsObject(
                        event.player.uniqueId,
                        stat,
                        null,
                        null,
                        event.player.getStatistic(stat)
                ))
            }
        }
    }

    @EventHandler
    fun onLeave(event: PlayerQuitEvent) {
        statsDb.scope.launch {
            StatsDB.UPDATE_STATISTICS.forEach { stat ->
                statisticsQueue.add(StatisticsObject(
                        event.player.uniqueId,
                        stat, null, null,
                        event.player.getStatistic(stat)
                ))
            }
        }
    }
}
