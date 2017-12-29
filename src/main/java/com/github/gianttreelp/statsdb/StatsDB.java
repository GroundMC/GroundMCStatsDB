package com.github.gianttreelp.statsdb;

import org.apache.commons.dbcp2.BasicDataSource;
import org.bukkit.Bukkit;
import org.bukkit.Statistic;
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

    protected static final List<Statistic> STATISTIC_LIST = Arrays.asList(
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
    private static DataSource dataSource;
    private EventListener eventListener;
    private Lock syncLock = new ReentrantLock();

    private static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void onEnable() {
        saveDefaultConfig();
        dataSource = getDataSource();

        if (dataSource == null) {
            getPluginLoader().disablePlugin(this);
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
        String serverIdentifier = getConfig().getString("server.identifier");
        try {
            Connection connection = getConnection();
            if (connection == null || !syncLock.tryLock()) {
                return;
            }

            PreparedStatement updates = connection.prepareStatement(
                    "UPDATE `Statistics`" +
                            "SET `value` = ? " +
                            "WHERE `server_id` = ? " +
                            "AND `player_id` = ?" +
                            "AND `statistic` = ?");

            Bukkit.getOnlinePlayers().forEach(player -> {
                UUID playerId = player.getUniqueId();
                byte[] uuid = new byte[16];
                ByteBuffer.wrap(uuid).order(ByteOrder.BIG_ENDIAN)
                        .putLong(playerId.getMostSignificantBits())
                        .putLong(playerId.getLeastSignificantBits());
                STATISTIC_LIST.forEach(statistic -> {
                    try {
                        updates.setInt(1, player.getStatistic(statistic));
                        updates.setString(2, serverIdentifier);
                        updates.setBytes(3, uuid);
                        updates.setString(4, statistic.name());
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
                connection.commit();
            }
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

    private DataSource getDataSource() {
        BasicDataSource source = new BasicDataSource();
        source.setUrl(getConfig().getString("database.url"));
        source.setUsername(getConfig().getString("database.username"));
        source.setPassword(getConfig().getString("database.password"));
        source.setDefaultAutoCommit(false);
        source.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        return source;
    }
}
