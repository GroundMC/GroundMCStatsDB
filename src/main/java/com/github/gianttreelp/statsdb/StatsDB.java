package com.github.gianttreelp.statsdb;

import org.bukkit.Bukkit;
import org.bukkit.configuration.Configuration;
import org.bukkit.plugin.java.JavaPlugin;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import static org.bukkit.Statistic.*;

public class StatsDB extends JavaPlugin {

    private static Connection connection;
    private static Configuration configuration;

    public static Connection getConnection() {
        return connection;
    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void onEnable() {
        saveDefaultConfig();
        connection = openConnection();
        configuration = getConfig();

        if (connection == null) {
            getPluginLoader().disablePlugin(this);
        }

        createTable();
        registerTasks();
        registerEventListener();
    }

    private void registerEventListener() {
        Bukkit.getPluginManager().registerEvents(new EventListener(), this);
    }

    private void registerTasks() {
        Bukkit.getScheduler().scheduleSyncRepeatingTask(this,
                () -> Bukkit.getOnlinePlayers().forEach(player -> {
                    UUID playerId = player.getUniqueId();
                    byte[] uuid = new byte[16];
                    ByteBuffer.wrap(uuid).order(ByteOrder.BIG_ENDIAN)
                            .putLong(playerId.getMostSignificantBits())
                            .putLong(playerId.getLeastSignificantBits());
                    String serverIdentifier = getConfig().getString("server.identifier");
                    Arrays.asList(
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
                            AVIATE_ONE_CM).forEach(statistic -> {
                        try {
                            PreparedStatement statement = connection.prepareStatement(
                                    "DELETE FROM `Statistics` WHERE " +
                                            "`server_id` = ? " +
                                            "AND `player_id` = ?" +
                                            "AND `statistic` = ?");
                            statement.setString(1, serverIdentifier);
                            statement.setBytes(2, uuid);
                            statement.setString(3, statistic.name());
                            statement.execute();

                            statement = connection.prepareStatement(
                                    "INSERT INTO `Statistics`(" +
                                            "`server_id`, `player_id`, `statistic`, `value`) " +
                                            "VALUES (?, ?, ?, ?)");
                            statement.setString(1, serverIdentifier);
                            statement.setBytes(2, uuid);
                            statement.setString(3, statistic.name());
                            statement.setInt(4, player.getStatistic(statistic));
                            statement.execute();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
                }),
                getConfig().getLong("server.sync_interval"),
                getConfig().getLong("server.sync_interval"));
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
