package net.groundmc.statsdb

import com.google.common.collect.Queues
import kotlinx.coroutines.experimental.async
import org.bukkit.Material
import org.bukkit.Statistic
import org.bukkit.entity.EntityType
import org.bukkit.event.EventHandler
import org.bukkit.event.Listener
import org.bukkit.event.player.PlayerJoinEvent
import org.bukkit.event.player.PlayerQuitEvent
import org.bukkit.event.player.PlayerStatisticIncrementEvent
import java.sql.PreparedStatement
import java.util.*

internal class EventListener(private val statsDb: StatsDB) : Listener {

    val statisticsQueue: Queue<StatisticsObject> = Queues.newConcurrentLinkedQueue<StatisticsObject>()

    @EventHandler
    fun onStatisticIncrement(event: PlayerStatisticIncrementEvent) {
        val uuid = StatsDB.getBytesFromUUID(event.player)

        val statObject = statisticsQueue.asSequence()
                .firstOrNull { (uuid1, statistic, material, entity) ->
                    Arrays.equals(uuid1, uuid) &&
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

    private fun getSelectStatistics(): PreparedStatement {
        val connection = statsDb.getConnection()
                ?: throw IllegalStateException()
        return connection.prepareStatement(
                "SELECT * FROM `Statistics` WHERE `player_id` = ?")
    }

    @EventHandler
    fun onJoin(event: PlayerJoinEvent) {
        async {
            val uuid = StatsDB.getBytesFromUUID(event.player)
            getSelectStatistics().use { query ->
                query.setBytes(1, uuid)
                query.executeQuery().use { rs ->
                    while (rs.next()) {
                        val statistic = Statistic.valueOf(rs.getString("statistic"))
                        when (statistic.type) {
                            Statistic.Type.UNTYPED -> event.player.setStatistic(statistic, rs.getInt("value"))
                            Statistic.Type.ENTITY -> {
                                val entity = EntityType.valueOf(rs.getString("entity"))
                                event.player.setStatistic(statistic, entity, rs.getInt("value"))
                            }
                            Statistic.Type.BLOCK, Statistic.Type.ITEM -> {
                                val material = Material.valueOf(rs.getString("material"))
                                event.player.setStatistic(statistic, material, rs.getInt("value"))
                            }
                            null -> {
                            }
                        }
                    }
                }
            }
            StatsDB.STATISTIC_LIST.forEach { stat ->
                statisticsQueue.add(StatisticsObject(
                        uuid,
                        stat, null, null,
                        event.player.getStatistic(stat)
                ))
            }
        }.start()
    }

    @EventHandler
    fun onLeave(event: PlayerQuitEvent) {
        async {
            val uuid = StatsDB.getBytesFromUUID(event.player)
            StatsDB.STATISTIC_LIST.forEach { stat ->
                statisticsQueue.add(StatisticsObject(
                        uuid,
                        stat, null, null,
                        event.player.getStatistic(stat)
                ))
            }
        }.start()
    }
}
