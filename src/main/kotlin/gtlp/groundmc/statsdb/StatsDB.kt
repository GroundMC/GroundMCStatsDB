package gtlp.groundmc.statsdb

import com.google.common.collect.Maps
import org.bukkit.Bukkit
import org.bukkit.OfflinePlayer
import org.bukkit.Statistic.*
import org.bukkit.plugin.java.JavaPlugin
import org.bukkit.scheduler.BukkitTask
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.sql.*
import java.util.*
import java.util.concurrent.locks.ReentrantLock

class StatsDB : JavaPlugin() {

    private val syncLock = ReentrantLock()
    private var eventListener = EventListener()
    private val statementMap by lazy { prepareStatements(connection) }
    private lateinit var task: BukkitTask

    override fun onEnable() {
        saveDefaultConfig()

        connection = DriverManager.getConnection(config.getString("database.url"),
                Properties().apply {
                    put("user", config.getString("database.username"))
                    put("password", config.getString("database.password"))
                    put("journal_mode", "wal")
                }).apply {
            transactionIsolation = Connection.TRANSACTION_SERIALIZABLE
            this.autoCommit = false
        }

        createTable()
        registerTasks()
        registerEventListener()
        registerCommand()
    }

    override fun onDisable() {
        task.cancel()
        synchronizeStats()
        Companion.connection.close()
    }

    private fun registerCommand() {
        val command = getCommand("statsdb")
        val dbCommand = StatsDBCommand(this)
        command.executor = dbCommand
        command.tabCompleter = dbCommand
    }

    private fun registerEventListener() {
        Bukkit.getPluginManager().registerEvents(eventListener, this)
    }

    private fun registerTasks() {
        task = Bukkit.getScheduler().runTaskTimerAsynchronously(this,
                this::synchronizeStats,
                config.getLong("server.sync_interval"),
                config.getLong("server.sync_interval"))
    }

    private fun synchronizeStats() {
        if (syncLock.isLocked) {
            return
        }
        if (!syncLock.tryLock()) {
            return
        }
        try {
            if (!addBatches()) return
            statementMap.values.forEach {
                it.executeBatch()
            }
            connection.commit()
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            syncLock.unlock()
        }
    }

    private fun addBatches(): Boolean {
        val updates = statementMap[SqlType.UPDATE] ?: return false
        Bukkit.getOnlinePlayers().forEach { player ->
            val uuid = getBytesFromUUID(player)
            for (stat in STATISTIC_LIST) {
                updates.setInt(1, player.getStatistic(stat))
                updates.setBytes(2, uuid)
                updates.setString(3, stat.name)
                updates.addBatch()
            }
        }

        val insertStatement = statementMap[SqlType.INSERT]
                ?: return false

        while (eventListener.statisticsQueue.peek() != null) {
            val stat = eventListener.statisticsQueue.poll()
            val deleteStatement = statementMap[stat.statistic.type]
                    ?: return false
            deleteStatement.setBytes(1, stat.uuid)
            deleteStatement.setString(2, stat.statistic.name)
            if (stat.material != null) {
                deleteStatement.setString(3, stat.material)
            } else if (stat.entity != null) {
                deleteStatement.setString(3, stat.entity)
            }
            deleteStatement.addBatch()

            insertStatement.setBytes(1, stat.uuid)
            insertStatement.setString(2, stat.statistic.name)
            insertStatement.setInt(3, stat.value)
            when {
                stat.material != null -> insertStatement.setString(4, stat.material)
                else -> insertStatement.setNull(4, Types.VARCHAR)
            }
            when {
                stat.entity != null -> insertStatement.setString(5, stat.entity)
                else -> insertStatement.setNull(5, Types.VARCHAR)
            }
            insertStatement.addBatch()
        }
        return true
    }

    private fun createTable() {
        try {
            Companion.connection.createStatement().execute("CREATE TABLE " +
                    "IF NOT EXISTS `Statistics`(" +
                    "`player_id` BINARY(16) NOT NULL ," +
                    "`statistic` VARCHAR(255) NOT NULL ," +
                    "`material` VARCHAR(255)," +
                    "`entity` VARCHAR(255)," +
                    "`value` BIGINT NOT NULL)")
        } catch (e: SQLException) {
            e.printStackTrace()
        }
    }

    companion object {

        internal val STATISTIC_LIST = arrayOf(
                PLAY_ONE_TICK,
                WALK_ONE_CM,
                SWIM_ONE_CM,
                FALL_ONE_CM,
                SNEAK_TIME,
                CLIMB_ONE_CM,
                FLY_ONE_CM,
                DIVE_ONE_CM,
                MINECART_ONE_CM,
                BOAT_ONE_CM,
                PIG_ONE_CM,
                HORSE_ONE_CM,
                SPRINT_ONE_CM,
                CROUCH_ONE_CM,
                AVIATE_ONE_CM,
                TIME_SINCE_DEATH)
        private val statementStringMap = linkedMapOf(
                Type.UNTYPED to "DELETE FROM `Statistics` WHERE " +
                        "`player_id` = ? AND `statistic` = ?;",
                Type.BLOCK to "DELETE FROM `Statistics` WHERE " +
                        "`player_id` = ? " +
                        "AND `statistic` = ? " +
                        "AND `material` = ?;",
                Type.ITEM to "DELETE FROM `Statistics` WHERE " +
                        "`player_id` = ? " +
                        "AND `statistic` = ? " +
                        "AND `material` = ?;",
                Type.ENTITY to "DELETE FROM `Statistics` WHERE " +
                        "`player_id` = ? " +
                        "AND `statistic` = ? " +
                        "AND `entity` = ?;",
                SqlType.INSERT to "INSERT INTO `Statistics`(" +
                        "`player_id`, `statistic`, " +
                        "`value`, `material`, `entity`) " +
                        "VALUES (?, ?, ?, ?, ?);",
                SqlType.UPDATE to "UPDATE `Statistics` " +
                        "SET `value` = ? " +
                        "WHERE `player_id` = ? " +
                        "AND `statistic` = ?;"
        )

        internal fun getBytesFromUUID(player: OfflinePlayer) =
                ByteBuffer.wrap(ByteArray(16)).order(ByteOrder.BIG_ENDIAN)
                        .putLong(player.uniqueId.mostSignificantBits)
                        .putLong(player.uniqueId.leastSignificantBits).array()

        @Throws(SQLException::class)
        private fun prepareStatements(connection: Connection) =
                Maps.newLinkedHashMap<Enum<*>, PreparedStatement>().apply {
                    for (type in Type.values()) {
                        this[type] = connection.prepareStatement(
                                statementStringMap[type])
                    }
                    for (type in SqlType.values()) {
                        this[type] = connection.prepareStatement(
                                statementStringMap[type])
                    }
                }

        internal lateinit var connection: Connection
    }
}
