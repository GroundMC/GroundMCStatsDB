package net.groundmc.statsdb

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import net.groundmc.statsdb.database.Statistics
import org.bukkit.Bukkit
import org.bukkit.Statistic.*
import org.bukkit.plugin.java.JavaPlugin
import org.bukkit.scheduler.BukkitRunnable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class StatsDB : JavaPlugin() {

    val scope = CoroutineScope(Dispatchers.Default)
    val statistics = Statistics()
    lateinit var database: Database

    private val syncLock = ReentrantLock()
    private val task = this::synchronizeStats as BukkitRunnable
    private val eventListener = EventListener(this)

    private lateinit var dataSource: HikariDataSource

    override fun onEnable() {
        saveDefaultConfig()

        val hikariConfig = HikariConfig().apply {
            jdbcUrl = config.getString("database.url")
                    .replace("\$dataFolder", dataFolder.absolutePath)
            username = config.getString("database.username", "")
            password = config.getString("database.password", "")
            transactionIsolation = "TRANSACTION_READ_COMMITTED"
            isAutoCommit = false
        }

        dataSource = HikariDataSource(hikariConfig)

        database = Database.connect(dataSource)
        transaction(database) {
            SchemaUtils.createMissingTablesAndColumns(statistics)
        }
        registerTasks()
        registerEventListener()
        registerCommand()
    }

    override fun onDisable() {
        task.cancel()
        scope.cancel("Shutting down...")
        dataSource.close()
        synchronizeStats()
    }

    private fun registerCommand() {
        val command = getCommand("statsdb")
        val dbCommand = StatsDBCommand(this, statistics)
        command.executor = dbCommand
        command.tabCompleter = dbCommand
    }

    private fun registerEventListener() {
        Bukkit.getPluginManager().registerEvents(eventListener, this)
    }

    private fun registerTasks() {
        task.runTaskTimerAsynchronously(
                this,
                config.getLong("server.sync_interval"),
                config.getLong("server.sync_interval")
        )
    }

    private fun synchronizeStats() {
        if (syncLock.isLocked) {
            logger.warning("Syncing is still ongoing! High load?")
            return
        }
        if (!syncLock.tryLock(10, TimeUnit.SECONDS)) {
            logger.warning("Syncing was locked for 10 seconds! Check your connection to the database!")
            return
        }
        try {
            transaction(database) {
                val savepoint = connection.setSavepoint()
                try {
                    Bukkit.getOnlinePlayers().forEach { player ->
                        UPDATE_STATISTICS.forEach { stat ->
                            statistics.update({
                                (statistics.uuid eq player.uniqueId) and
                                        (statistics.statistic eq stat)
                            }) {
                                it[uuid] = player.uniqueId
                                it[statistic] = stat
                            }
                        }
                    }
                    val queue = eventListener.statisticsQueue.toCollection(ArrayBlockingQueue(eventListener.statisticsQueue.size))
                    queue.forEach {
                        statistics.deleteWhere {
                            (statistics.uuid eq it.uuid) and
                                    (statistics.statistic eq it.statistic) and
                                    if (it.material != null) {
                                        statistics.material eq it.material
                                    } else {
                                        Op.TRUE
                                    } and if (it.entity != null) {
                                statistics.entity eq it.entity
                            } else {
                                Op.TRUE
                            }
                        }
                    }
                    eventListener.statisticsQueue.removeAll(queue)
                    commit()
                    connection.releaseSavepoint(savepoint)
                } catch (e: Exception) {
                    connection.rollback(savepoint)
                }


            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            syncLock.unlock()
        }
    }

    companion object {

        internal val UPDATE_STATISTICS = arrayListOf(
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
    }
}
