package gtlp.groundmc.statsdb

import org.bukkit.Bukkit
import org.bukkit.Material
import org.bukkit.Statistic
import org.bukkit.command.Command
import org.bukkit.command.CommandExecutor
import org.bukkit.command.CommandSender
import org.bukkit.command.TabCompleter
import org.bukkit.entity.EntityType
import org.bukkit.entity.Player
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.*

@Suppress("DEPRECATION")
internal class StatsDBCommand(private val statsDB: StatsDB) : CommandExecutor, TabCompleter {

    private fun startsWithIgnoreCase(candidate: String, prefix: String): Boolean {
        return candidate.toLowerCase(Locale.ROOT).startsWith(prefix.toLowerCase(Locale.ROOT))
    }

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

    private fun playerSubstatistic(sender: CommandSender, args: Array<String>) {
        try {
            val stat = Statistic.valueOf(args[0])
            val builder = StringBuilder(
                    "SELECT `value` FROM `Statistics` " +
                            "WHERE `player_id` = ? " +
                            "AND `statistic` = ? ")
            val connection = StatsDB.connection
            if (sender is Player) {

                val player = Bukkit.getOfflinePlayer(args[2])

                appendSubstatisticSQL(stat, builder)

                val statement = connection.prepareStatement(
                        builder.toString())
                statement.setBytes(1, StatsDB.getBytesFromUUID(player))
                statement.setString(2, stat.name)
                statement.setString(3, args[1])
                executeSendAndClose(sender, stat, statement)
            }
        } catch (e: SQLException) {
            e.printStackTrace()
        }

    }

    @Throws(SQLException::class)
    private fun executeSendAndClose(sender: CommandSender, stat: Statistic, statement: PreparedStatement) {
        val result = statement.executeQuery()
        if (result.next()) {
            sender.sendMessage(stat.name + ": " + result.getString(1))
        }
        result.close()
        statement.close()
    }

    private fun subStatisticOrPlayerStatistic(sender: CommandSender, args: Array<String>) {
        try {
            val stat = Statistic.valueOf(args[0])
            val builder = StringBuilder(
                    "SELECT `value` FROM `Statistics` " +
                            "WHERE `player_id` = ? " +
                            "AND `statistic` = ? ")
            val connection = StatsDB.connection
            if (stat.isSubstatistic) {
                if (sender is Player) {

                    appendSubstatisticSQL(stat, builder)

                    val statement = connection.prepareStatement(
                            builder.toString())
                    statement.setBytes(1, StatsDB.getBytesFromUUID(sender))
                    statement.setString(2, stat.name)
                    statement.setString(3, args[1])
                    executeSendAndClose(sender, stat, statement)
                }
            } else {

                val player = Bukkit.getOfflinePlayer(args[1])
                val statement = connection.prepareStatement(
                        builder.toString())
                statement.setBytes(1, StatsDB.getBytesFromUUID(player))
                statement.setString(2, stat.name)
                val result = statement.executeQuery()
                if (result.next()) {
                    sender.sendMessage(stat.name + ": " + result.getString(1))
                } else {
                    sender.sendMessage("Statistic not recorded yet.")
                }
                result.close()
                statement.close()
            }
            connection.close()
        } catch (e: IllegalArgumentException) {
            sender.sendMessage("Unrecognized statistic")
        } catch (e: SQLException) {
            e.printStackTrace()
        }

    }

    private fun appendSubstatisticSQL(stat: Statistic, builder: StringBuilder) {
        when (stat.type) {
            Statistic.Type.ITEM, Statistic.Type.BLOCK -> builder.append("AND `material` = ?")
            Statistic.Type.ENTITY -> builder.append("AND `entity` = ?")
            else -> {
            }
        }
    }

    private fun selfStatistic(sender: CommandSender, args: Array<String>) {
        try {
            val stat = Statistic.valueOf(args[0])
            if (sender is Player) {
                val connection = StatsDB.connection
                val statement = connection.prepareStatement(
                        "SELECT `value` FROM `Statistics` " +
                                "WHERE `player_id` = ?" +
                                "AND `statistic` = ?"
                )
                statement.setBytes(1, StatsDB.getBytesFromUUID(sender))
                statement.setString(2, stat.name)
                executeSendAndClose(sender, stat, statement)
                connection.close()
            }
        } catch (e: IllegalArgumentException) {
            sender.sendMessage("Unrecognized statistic")
        } catch (e: SQLException) {
            e.printStackTrace()
        }

    }

    private fun version(sender: CommandSender) {
        if (sender.hasPermission(Bukkit.getPluginManager().getPermission("statsdb.admin"))) {
            sender.sendMessage("Running StatsDB commit " + statsDB.description.version)
        }
    }

    override fun onTabComplete(sender: CommandSender, command: Command, alias: String, args: Array<String>): List<String>? {
        when (args.size) {
            1 -> return getStatisticNameBeginningWith(args[0])
            2 -> {
                return when (Statistic.valueOf(args[0]).type) {
                    Statistic.Type.UNTYPED -> getPlayerNamesBeginningWith(args[1])
                    Statistic.Type.BLOCK -> getBlockNamesBeginningWith(args[1])
                    Statistic.Type.ITEM -> getItemNamesBeginningWith(args[1])
                    Statistic.Type.ENTITY -> getEntityNamesBeginningWith(args[1])
                    null -> null
                    else -> getPlayerNamesBeginningWith(args[2])
                }
            }
            3 -> return getPlayerNamesBeginningWith(args[2])
        }
        return null
    }

    private fun getEntityNamesBeginningWith(begin: String): List<String> {
        val list = ArrayList<String>(EntityType.values().size / 3)
        for (entityType in EntityType.values()) {
            val name = entityType.name
            if (startsWithIgnoreCase(name, begin)) {
                list.add(name)
            }
        }
        return list
    }

    private fun getItemNamesBeginningWith(beign: String): List<String> {
        val list = ArrayList<String>()
        for (material in Material.values()) {
            if (!material.isBlock) {
                val name = material.name
                if (startsWithIgnoreCase(name, beign)) {
                    list.add(name)
                }
            }
        }
        return list
    }

    private fun getStatisticNameBeginningWith(begin: String): List<String> {
        val list = ArrayList<String>(Statistic.values().size / 3)
        for (statistic in Statistic.values()) {
            val name = statistic.name
            if (startsWithIgnoreCase(name, begin)) {
                list.add(name)
            }
        }
        return list
    }

    private fun getBlockNamesBeginningWith(begin: String): List<String> {
        val list = ArrayList<String>()
        for (material in Material.values()) {
            if (material.isBlock) {
                val name = material.name
                if (startsWithIgnoreCase(name, begin)) {
                    list.add(name)
                }
            }
        }
        return list
    }

    private fun getPlayerNamesBeginningWith(begin: String): List<String> {
        val list = ArrayList<String>(Bukkit.getOnlinePlayers().size / 3)
        for (player in Bukkit.getOnlinePlayers()) {
            val name = player.displayName
            if (startsWithIgnoreCase(name, begin)) {
                list.add(name)
            }
        }
        return list
    }
}
