package net.groundmc.statsdb

import net.groundmc.statsdb.database.Statistics
import org.bukkit.Bukkit
import org.bukkit.Material
import org.bukkit.Statistic
import org.bukkit.command.Command
import org.bukkit.command.CommandExecutor
import org.bukkit.command.CommandSender
import org.bukkit.command.TabCompleter
import org.bukkit.entity.EntityType
import org.bukkit.entity.Player
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction

@Suppress("DEPRECATION")
internal class StatsDBCommand(private val statsDB: StatsDB,
                              private val statistics: Statistics) : CommandExecutor, TabCompleter {

    override fun onCommand(sender: CommandSender, command: Command, label: String, args: Array<String>): Boolean {
        Bukkit.getScheduler().runTaskAsynchronously(statsDB) {
            when (args.size) {
                0 -> version(sender)
                1 -> selfStatistic(sender, args)
                2 -> subStatisticOrPlayerStatistic(sender, args)
                3 -> playerSubstatistic(sender, args)
            }
        }
        return true
    }

    private fun selfStatistic(sender: CommandSender, args: Array<String>) {
        if (sender !is Player) {
            return
        }
        val stat = Statistic.valueOf(args[0])
        transaction(statsDB.database)
        statistics.select {
            (statistics.uuid eq sender.uniqueId) and
                    (statistics.statistic eq stat)
        }.map {
            StatisticsObject(
                    it[statistics.uuid],
                    it[statistics.statistic],
                    it[statistics.material],
                    it[statistics.entity],
                    it[statistics.value]
            )
        }
    }.forEach
    {
        sender.sendMessage("${it.statistic.name}: ${it.value}")
    }
}

    private fun playerSubstatistic(sender: CommandSender, args: Array<String>) {
        val player = Bukkit.getOfflinePlayer(args[2])
            val stat = Statistic.valueOf(args[0])

        statistics.runCatching {
            transaction(statsDB.database)
            select {
                (uuid eq player.uniqueId) and
                        (statistic eq stat) and
                        when (stat.type) {
                            Statistic.Type.ITEM, Statistic.Type.BLOCK -> {
                                material eq Material.valueOf(args[1])
                            }
                            Statistic.Type.ENTITY -> {
                                entity eq EntityType.valueOf(args[1])
                            }
                            else -> Op.TRUE
                        }
            }
        }
    }.onSuccess {
    query ->
    query.map {
        StatisticsObject(
                it[statistics.uuid],
                it[statistics.statistic],
                it[statistics.material],
                it[statistics.entity],
                it[statistics.value]
        )
    }.forEach {
        sender.sendMessage("${it.statistic.name}: ${it.value}")
    }
}.onFailure {
    it.printStackTrace()
        }
    }

    private fun subStatisticOrPlayerStatistic(sender: CommandSender, args: Array<String>) {
        val stat = Statistic.valueOf(args[0])
        if (stat.isSubstatistic) {
            playerSubstatistic(sender, arrayOf(args[0], args[1], sender.name))
        } else {
            val player: Player? = Bukkit.getPlayer(args[1])
            if (player == null) {
                sender.sendMessage("This player is not online, can't get statistics")
                return
            }
            transaction(statsDB.database) {
                statistics.select {
                    (statistics.uuid eq player.uniqueId) and
                            (statistics.statistic eq stat)
                }
            }.let { query ->
                if (query.empty()) {
                    sender.sendMessage("Statistic not recorded yet.")
                } else {
                    query.map {
                        StatisticsObject(
                                it[statistics.uuid],
                                it[statistics.statistic],
                                it[statistics.material],
                                it[statistics.entity],
                                it[statistics.value]
                        )
                    }.forEach {
                        sender.sendMessage("${it.statistic.name}: ${it.value}")
                    }
                }
            }
        }
    }

    private fun version(sender: CommandSender) {
        sender.sendMessage("Running StatsDB commit " + statsDB.description.version)
    }

    override fun onTabComplete(sender: CommandSender, command: Command, alias: String, args: Array<String>): List<String>? {
        when (args.size) {
            1 -> return getStatisticNameBeginningWith(args[0])
            2 -> {
                return when (Statistic.valueOf(args[0]).type) {
                    Statistic.Type.BLOCK -> getBlockNamesBeginningWith(args[1])
                    Statistic.Type.ITEM -> getItemNamesBeginningWith(args[1])
                    Statistic.Type.ENTITY -> getEntityNamesBeginningWith(args[1])
                    null -> null
                    else -> getPlayerNamesBeginningWith(args[1])
                }
            }
            3 -> return getPlayerNamesBeginningWith(args[2])
        }
        return null
    }

    private fun getEntityNamesBeginningWith(begin: String) =
            EntityType.values().asSequence()
                    .map { it.name }
                    .filter { it.startsWith(begin, true) }
                    .toList()

    private fun getItemNamesBeginningWith(begin: String) =
            Material.values().asSequence()
                    .filterNot { it.isBlock }
                    .map { it.name }
                    .filter { it.startsWith(begin, true) }
                    .toList()

    private fun getStatisticNameBeginningWith(begin: String) =
            Statistic.values().asSequence()
                    .map { it.name }
                    .filter { it.startsWith(begin, true) }
                    .toList()

    private fun getBlockNamesBeginningWith(begin: String) =
            Material.values().asSequence()
                    .filter { it.isBlock }
                    .map { it.name }
                    .filter { it.startsWith(begin, true) }
                    .toList()

    private fun getPlayerNamesBeginningWith(begin: String) =
            Bukkit.getOnlinePlayers().asSequence()
                    .map { it.displayName }
                    .filter { it.startsWith(begin, true) }
                    .toList()
}
