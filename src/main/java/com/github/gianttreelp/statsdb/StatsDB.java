package com.github.gianttreelp.statsdb;

import org.bukkit.Bukkit;
import org.bukkit.Statistic;
import org.bukkit.plugin.java.JavaPlugin;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.bukkit.Statistic.*;

@SuppressWarnings("unused")
public class StatsDB extends JavaPlugin {

    private static final List<Statistic> STATISTIC_LIST = Arrays.asList(
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
            AVIATE_ONE_CM);
    private static Connection connection;
    private EventListener eventListener;

    private Lock syncLock = new ReentrantLock();

    @Override
    public void onEnable() {
        saveDefaultConfig();
        connection = openConnection();

        if (connection == null) {
            getPluginLoader().disablePlugin(this);
        }
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        createTable();
        registerTasks();
        registerEventListener();
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
        if (!syncLock.tryLock()) {
            return;
        }
        String serverIdentifier = getConfig().getString("server.identifier");
        try {
            PreparedStatement deletes = connection.prepareStatement(
                    "DELETE FROM `Statistics` WHERE " +
                            "`server_id` = ? " +
                            "AND `player_id` = ?" +
                            "AND `statistic` = ?");

            PreparedStatement inserts = connection.prepareStatement(
                    "INSERT INTO `Statistics`(" +
                            "`server_id`, `player_id`, `statistic`, `value`) " +
                            "VALUES (?, ?, ?, ?)");

            Bukkit.getOnlinePlayers().forEach(player -> {
                UUID playerId = player.getUniqueId();
                byte[] uuid = new byte[16];
                ByteBuffer.wrap(uuid).order(ByteOrder.BIG_ENDIAN)
                        .putLong(playerId.getMostSignificantBits())
                        .putLong(playerId.getLeastSignificantBits());
                STATISTIC_LIST.forEach(statistic -> {
                    try {
                        deletes.setString(1, serverIdentifier);
                        deletes.setBytes(2, uuid);
                        deletes.setString(3, statistic.name());
                        deletes.addBatch();


                        inserts.setString(1, serverIdentifier);
                        inserts.setBytes(2, uuid);
                        inserts.setString(3, statistic.name());
                        inserts.setInt(4, player.getStatistic(statistic));
                        inserts.addBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            });

            deletes.executeBatch();
            inserts.executeBatch();

            StatisticsObject stat;
            while ((stat = eventListener.statisticsQueue.poll()) != null) {
                StringBuilder builder = new StringBuilder()
                        .append("DELETE FROM `Statistics` WHERE ")
                        .append("`server_id` = ? ")
                        .append("AND `player_id` = ? ")
                        .append("AND `statistic` = ? ");

                if (stat.material != null) {
                    builder.append("AND `material` = ? ");
                }
                if (stat.entity != null) {
                    builder.append("AND `entity` = ? ");
                }
                try {
                    PreparedStatement statement = connection.prepareStatement(builder.toString());
                    statement.setString(1, serverIdentifier);
                    statement.setBytes(2, stat.uuid);
                    statement.setString(3, stat.statistic);
                    if (stat.material != null) {
                        statement.setString(4, stat.material);
                    } else if (stat.entity != null) {
                        statement.setString(4, stat.entity);
                    }
                    statement.executeUpdate();
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO `Statistics`(" +
                            "`server_id`, `player_id`, `statistic`, `value`, `material`, `entity`) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");
                    insertStatement.setString(1, serverIdentifier);
                    insertStatement.setBytes(2, stat.uuid);
                    insertStatement.setString(3, stat.statistic);
                    insertStatement.setInt(4, stat.value);
                    if (stat.material != null) {
                        insertStatement.setString(5, stat.material);
                    } else {
                        insertStatement.setNull(5, Types.VARCHAR);
                    }
                    if (stat.entity != null) {
                        insertStatement.setString(6, stat.entity);
                    } else {
                        insertStatement.setNull(6, Types.VARCHAR);
                    }
                    insertStatement.executeUpdate();
                    insertStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            syncLock.unlock();
        }
    }

    private void createTable() {
        try {
            connection.createStatement().execute("CREATE TABLE " +
                    "IF NOT EXISTS `Statistics`(" +
                    "`server_id` VARCHAR(255) NOT NULL ," +
                    "`player_id` BINARY(16) NOT NULL ," +
                    "`statistic` VARCHAR(255) NOT NULL ," +
                    "`material` VARCHAR(255)," +
                    "`entity` VARCHAR(255)," +
                    "`value` BIGINT NOT NULL)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection openConnection() {
        try {
            return connection = DriverManager.getConnection(
                    getConfig().getString("database.url"),
                    getConfig().getString("database.username"),
                    getConfig().getString("database.password"));
        } catch (SQLException e) {
            e.printStackTrace();
            getPluginLoader().disablePlugin(this);
        }
        return null;
    }
}
