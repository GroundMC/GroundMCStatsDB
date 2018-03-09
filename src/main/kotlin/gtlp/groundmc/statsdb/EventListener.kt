package gtlp.groundmc.statsdb

import com.google.common.collect.Queues
import org.bukkit.Material
import org.bukkit.Statistic
import org.bukkit.entity.EntityType
import org.bukkit.event.EventHandler
import org.bukkit.event.Listener
import org.bukkit.event.player.PlayerJoinEvent
import org.bukkit.event.player.PlayerQuitEvent
import org.bukkit.event.player.PlayerStatisticIncrementEvent
import java.sql.SQLException
import java.util.*

internal class EventListener : Listener {

    val statisticsQueue: Queue<StatisticsObject> = Queues.newConcurrentLinkedQueue()

    @EventHandler
    fun onStatisticIncrement(event: PlayerStatisticIncrementEvent) {
        val uuid = StatsDB.getBytesFromUUID(event.player)

        val statObject = statisticsQueue.stream()
                .filter { (uuid1, statistic, material, entity) ->
                    Arrays.equals(uuid1, uuid) &&
                            event.statistic !=
                            null && statistic == event.statistic &&
                            event.material != null && material == event.material.name &&
                            event.entityType != null && entity == event.entityType.name
                }
                .findFirst()

        if (statObject.isPresent) {
            statObject.get().value = event.newValue
        } else {
            statisticsQueue.add(StatisticsObject(
                    uuid,
                    event.statistic,
                    if (event.material != null) event.material.name else null,
                    if (event.entityType != null) event.entityType.name else null,
                    event.newValue
            ))
        }
    }

    @EventHandler
    fun onJoin(event: PlayerJoinEvent) {
        val uuid = StatsDB.getBytesFromUUID(event.player)
        try {
            val connection = StatsDB.connection
            val statement = connection.prepareStatement(
                    "SELECT * FROM `Statistics`" + "WHERE `player_id` = ?"
            )
            statement.setBytes(1, uuid)
            val rs = statement.executeQuery()
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
            rs.close()
            connection.close()
        } catch (e: SQLException) {
            e.printStackTrace()
        }

        StatsDB.STATISTIC_LIST.forEach { stat ->
            statisticsQueue.add(StatisticsObject(
                    uuid,
                    stat, null, null,
                    event.player.getStatistic(stat)
            ))
        }
    }

    @EventHandler
    fun onLeave(event: PlayerQuitEvent) {
        val uuid = StatsDB.getBytesFromUUID(event.player)
        StatsDB.STATISTIC_LIST.forEach { stat ->
            statisticsQueue.add(StatisticsObject(
                    uuid,
                    stat, null, null,
                    event.player.getStatistic(stat)
            ))
        }
    }
}
