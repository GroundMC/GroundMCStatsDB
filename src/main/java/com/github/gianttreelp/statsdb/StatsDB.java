package com.github.gianttreelp.statsdb;

import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.Statistic;
import org.bukkit.command.PluginCommand;
import org.bukkit.plugin.java.JavaPlugin;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.bukkit.Statistic.*;

@SuppressWarnings("unused")
public class StatsDB extends JavaPlugin {

    static final List<Statistic> STATISTIC_LIST = Arrays.asList(
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
            TIME_SINCE_DEATH);
    private static DataSource dataSource;
    private EventListener eventListener;
    private Lock syncLock = new ReentrantLock();

    static byte[] getBytesFromUUID(OfflinePlayer player) {
        UUID playerId = player.getUniqueId();
        return ByteBuffer.wrap(new byte[16]).order(ByteOrder.BIG_ENDIAN)
                .putLong(playerId.getMostSignificantBits())
                .putLong(playerId.getLeastSignificantBits()).array();
    }

    static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void onEnable() {
        saveDefaultConfig();
        dataSource = getDataSource();

        if (dataSource == null) {
            getLogger().severe("Can't connect to SQL Server, please " +
                    "verify your configuration.");
            getLogger().severe("Disabling...");
            getPluginLoader().disablePlugin(this);
        }

        createTable();
        registerTasks();
        registerEventListener();
        registerCommand();
    }

    @Override
    public void onDisable() {
        synchronizeStats();
    }

    private void registerCommand() {
        PluginCommand command = getCommand("statsdb");
        StatsDBCommand dbCommand = new StatsDBCommand(this);
        command.setExecutor(dbCommand);
        command.setTabCompleter(dbCommand);
    }

    private void registerEventListener() {
        eventListener = new EventListener();
        Bukkit.getPluginManager().registerEvents(eventListener, this);
    }

    private void registerTasks() {
        Bukkit.getScheduler().runTaskTimerAsynchronously(this,
                this::synchronizeStats,
                getConfig().getLong("server.sync_interval"),
                getConfig().getLong("server.sync_interval"));
    }

    private void synchronizeStats() {
        try {
            if (!syncLock.tryLock()) {
                return;
            }

            Connection connection = getConnection();
            if (connection == null) {
                return;
            }

            PreparedStatement updates = connection.prepareStatement(
                    "UPDATE `Statistics`" +
                            "SET `value` = ? " +
                            "WHERE `player_id` = ?" +
                            "AND `statistic` = ?");

            Bukkit.getOnlinePlayers().forEach(player -> {
                byte[] uuid = getBytesFromUUID(player);
                STATISTIC_LIST.forEach(statistic -> {
                    try {
                        updates.setInt(1, player.getStatistic(statistic));
                        updates.setBytes(2, uuid);
                        updates.setString(3, statistic.name());
                        updates.addBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            });

            updates.executeBatch();

            StatisticsObject stat;
            while ((stat = eventListener.statisticsQueue.poll()) != null) {
                StringBuilder builder = new StringBuilder()
                        .append("DELETE FROM `Statistics` WHERE " +
                                "`player_id` = ? " +
                                "AND `statistic` = ? ");

                if (stat.material != null) {
                    builder.append("AND `material` = ? ");
                }
                if (stat.entity != null) {
                    builder.append("AND `entity` = ? ");
                }
                try {
                    PreparedStatement statement = connection.prepareStatement(builder.toString());
                    statement.setBytes(1, stat.uuid);
                    statement.setString(2, stat.statistic);
                    if (stat.material != null) {
                        statement.setString(3, stat.material);
                    } else if (stat.entity != null) {
                        statement.setString(3, stat.entity);
                    }
                    statement.executeUpdate();
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    PreparedStatement insertStatement = connection.prepareStatement(
                            "INSERT INTO `Statistics`(" +
                                    "`player_id`, `statistic`, " +
                                    "`value`, `material`, `entity`) " +
                                    "VALUES (?, ?, ?, ?, ?)");
                    insertStatement.setBytes(1, stat.uuid);
                    insertStatement.setString(2, stat.statistic);
                    insertStatement.setInt(3, stat.value);
                    if (stat.material != null) {
                        insertStatement.setString(4, stat.material);
                    } else {
                        insertStatement.setNull(4, Types.VARCHAR);
                    }
                    if (stat.entity != null) {
                        insertStatement.setString(5, stat.entity);
                    } else {
                        insertStatement.setNull(5, Types.VARCHAR);
                    }
                    insertStatement.executeUpdate();
                    insertStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            connection.commit();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            syncLock.unlock();
        }
    }

    private void createTable() {
        try {
            getConnection().createStatement().execute("CREATE TABLE " +
                    "IF NOT EXISTS `Statistics`(" +
                    "`player_id` BINARY(16) NOT NULL ," +
                    "`statistic` VARCHAR(255) NOT NULL ," +
                    "`material` VARCHAR(255)," +
                    "`entity` VARCHAR(255)," +
                    "`value` BIGINT NOT NULL)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private DataSource getDataSource() {
        HikariDataSource source = new HikariDataSource();
        source.setJdbcUrl(getConfig().getString("database.url"));
        source.setUsername(getConfig().getString("database.username"));
        source.setPassword(getConfig().getString("database.password"));
        source.setAutoCommit(false);
        source.setTransactionIsolation("TRANSACTION_READ_UNCOMMITTED");
        source.addDataSourceProperty("journal_mode", "wal");
        return source;
    }
}
