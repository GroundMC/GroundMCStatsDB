package com.github.gianttreelp.statsdb;

import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.Statistic;
import org.bukkit.command.PluginCommand;
import org.bukkit.plugin.java.JavaPlugin;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.bukkit.Statistic.*;

@SuppressWarnings({"unused", "SqlDialectInspection", "SqlNoDataSourceInspection"})
public class StatsDB extends JavaPlugin {

    static final Statistic[] STATISTIC_LIST = {
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
            TIME_SINCE_DEATH};
    private static final Map<Object, String> statementStringMap = Maps.newConcurrentMap();
    private static HikariDataSource dataSource;

    static {
        statementStringMap.put(Type.UNTYPED, "DELETE FROM `Statistics` WHERE " +
                "`player_id` = ? AND `statistic` = ?;"
        );
        statementStringMap.put(Type.BLOCK, "DELETE FROM `Statistics` WHERE " +
                "`player_id` = ? " +
                "AND `statistic` = ?" +
                "AND `material` = ?;"
        );
        statementStringMap.put(Type.ITEM, statementStringMap.get(Type.BLOCK));
        statementStringMap.put(Type.ENTITY, "DELETE FROM `Statistics` WHERE " +
                "`player_id` = ? " +
                "AND `statistic` = ?" +
                "AND `entity` = ?;"
        );
        statementStringMap.put(SqlType.INSERT, "INSERT INTO `Statistics`(" +
                "`player_id`, `statistic`, " +
                "`value`, `material`, `entity`) " +
                "VALUES (?, ?, ?, ?, ?)");
        statementStringMap.put(SqlType.UPDATE, "UPDATE `Statistics`" +
                "SET `value` = ? " +
                "WHERE `player_id` = ?" +
                "AND `statistic` = ?");
    }

    final private Lock syncLock = new ReentrantLock();
    private EventListener eventListener;

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

        createTable();
        registerTasks();
        registerEventListener();
        registerCommand();
    }

    private Map<Object, PreparedStatement> prepareStatements(Connection connection) throws SQLException {
        Map<Object, PreparedStatement> map = Maps.newHashMap();
        for (Type type : Statistic.Type.values()) {
            map.put(type, connection.prepareStatement(
                    statementStringMap.get(type)));
        }
        for (SqlType type : SqlType.values()) {
            map.put(type, connection.prepareStatement(
                    statementStringMap.get(type)));
        }

        return map;
    }

    @Override
    public void onDisable() {
        synchronizeStats();
        dataSource.close();
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

            Map<Object, PreparedStatement> statementMap =
                    prepareStatements(connection);

            PreparedStatement updates = statementMap.get(SqlType.UPDATE);
            Bukkit.getOnlinePlayers().forEach(player -> {
                byte[] uuid = getBytesFromUUID(player);
                for (Statistic stat : STATISTIC_LIST) {
                    try {
                        updates.setInt(1, player.getStatistic(stat));
                        updates.setBytes(2, uuid);
                        updates.setString(3, stat.name());
                        updates.addBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });

            PreparedStatement insertStatement = statementMap.get(SqlType.INSERT);
            StatisticsObject stat;

            while ((stat = eventListener.statisticsQueue.poll()) != null) {
                try {
                    PreparedStatement deleteStatement = statementMap.get(
                            stat.statistic.getType());
                    deleteStatement.setBytes(1, stat.uuid);
                    deleteStatement.setString(2, stat.statistic.name());
                    if (stat.material != null) {
                        deleteStatement.setString(3, stat.material);
                    } else if (stat.entity != null) {
                        deleteStatement.setString(3, stat.entity);
                    }
                    deleteStatement.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    insertStatement.setBytes(1, stat.uuid);
                    insertStatement.setString(2, stat.statistic.name());
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
                    insertStatement.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            for (PreparedStatement statement : statementMap.values()) {
                statement.executeBatch();
                statement.close();
            }
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

    private HikariDataSource getDataSource() {
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
