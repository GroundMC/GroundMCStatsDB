package com.github.gianttreelp.statsdb;

import org.apache.commons.dbcp2.BasicDataSource;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.OfflinePlayer;
import org.bukkit.Statistic;
import org.bukkit.command.*;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
            AVIATE_ONE_CM,
            TIME_SINCE_DEATH);
    private static DataSource dataSource;
    private EventListener eventListener;
    private Lock syncLock = new ReentrantLock();

    static byte[] getBytesFromUUID(OfflinePlayer player) {
        UUID playerId = player.getUniqueId();
        byte[] uuid = new byte[16];
        ByteBuffer.wrap(uuid).order(ByteOrder.BIG_ENDIAN)
                .putLong(playerId.getMostSignificantBits())
                .putLong(playerId.getLeastSignificantBits());
        return uuid;
    }

    static Connection getConnection() throws SQLException {
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
        registerCommand();
    }

    @Override
    public void onDisable() {
        synchronizeStats();
    }

    private void registerCommand() {
        PluginCommand command = getCommand("statsdb");
        StatsDBCommand dbCommand = new StatsDBCommand();
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
            Connection connection = getConnection();
            if (connection == null || !syncLock.tryLock()) {
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
        BasicDataSource source = new BasicDataSource();
        source.setUrl(getConfig().getString("database.url"));
        source.setUsername(getConfig().getString("database.username"));
        source.setPassword(getConfig().getString("database.password"));
        source.setDefaultAutoCommit(false);
        source.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        source.addConnectionProperty("journal_mode", "wal");
        return source;
    }

    private class StatsDBCommand implements CommandExecutor, TabCompleter {
        @Override
        public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
            Bukkit.getScheduler().runTaskAsynchronously(StatsDB.this, () -> {
                switch (args.length) {
                    case 0:
                        version(sender);
                        break;
                    case 1:
                        selfStatistic(sender, args);
                        break;
                    case 2:
                        subStatisticOrPlayerStatistic(sender, args);
                        break;
                    case 3:
                        playerSubstatistic(sender, args);
                        break;
                }
            });
            return true;
        }

        private void playerSubstatistic(CommandSender sender, String[] args) {
            try {
                Statistic stat = Statistic.valueOf(args[0]);
                StringBuilder builder = new StringBuilder(
                        "SELECT `value` FROM `Statistics` " +
                                "WHERE `player_id` = ? " +
                                "AND `statistic` = ? ");
                Connection connection = getConnection();
                if (sender instanceof Player) {
                    //noinspection deprecation
                    OfflinePlayer player = Bukkit.getOfflinePlayer(args[2]);

                    appendSubstatisticSQL(stat, builder);

                    PreparedStatement statement = connection.prepareStatement(
                            builder.toString());
                    statement.setBytes(1, getBytesFromUUID(player));
                    statement.setString(2, stat.name());
                    statement.setString(3, args[1]);
                    ResultSet result = statement.executeQuery();
                    if (result.next()) {
                        sender.sendMessage(stat.name() + ": " + result.getString(1));
                    }
                    result.close();
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private void subStatisticOrPlayerStatistic(CommandSender sender, String[] args) {
            try {
                Statistic stat = Statistic.valueOf(args[0]);
                StringBuilder builder = new StringBuilder(
                        "SELECT `value` FROM `Statistics` " +
                                "WHERE `player_id` = ? " +
                                "AND `statistic` = ? ");
                Connection connection = getConnection();
                if (stat.isSubstatistic()) {
                    if (sender instanceof Player) {
                        Player player = (Player) sender;

                        appendSubstatisticSQL(stat, builder);

                        PreparedStatement statement = connection.prepareStatement(
                                builder.toString());
                        statement.setBytes(1, getBytesFromUUID(player));
                        statement.setString(2, stat.name());
                        statement.setString(3, args[1]);
                        ResultSet result = statement.executeQuery();
                        if (result.next()) {
                            sender.sendMessage(stat.name() + ": " + result.getString(1));
                        }
                        result.close();
                        statement.close();
                    }
                } else {
                    //noinspection deprecation
                    OfflinePlayer player = Bukkit.getOfflinePlayer(args[1]);
                    PreparedStatement statement = connection.prepareStatement(
                            builder.toString());
                    statement.setBytes(1, getBytesFromUUID(player));
                    statement.setString(2, stat.name());
                    ResultSet result = statement.executeQuery();
                    if (result.next()) {
                        sender.sendMessage(stat.name() + ": " + result.getString(1));
                    }
                    result.close();
                    statement.close();
                }
                connection.close();
            } catch (IllegalArgumentException e) {
                sender.sendMessage("Unrecognized statistic");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private void appendSubstatisticSQL(Statistic stat, StringBuilder builder) {
            switch (stat.getType()) {
                case ITEM:
                case BLOCK:
                    builder.append("AND `material` = ?");
                    break;
                case ENTITY:
                    builder.append("AND `entity` = ?");
                    break;
            }
        }

        private void selfStatistic(CommandSender sender, String[] args) {
            try {
                Statistic stat = Statistic.valueOf(args[0]);
                if (sender instanceof Player) {
                    Player player = (Player) sender;
                    Connection connection = getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "SELECT `value` FROM `Statistics` " +
                                    "WHERE `player_id` = ?" +
                                    "AND `statistic` = ?"
                    );
                    statement.setBytes(1, getBytesFromUUID(player));
                    statement.setString(2, stat.name());
                    ResultSet result = statement.executeQuery();
                    if (result.next()) {
                        sender.sendMessage(stat.name() + ": " + result.getString(1));
                    }
                    result.close();
                    statement.close();
                    connection.close();
                }
            } catch (IllegalArgumentException e) {
                sender.sendMessage("Unrecognized statistic");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private void version(CommandSender sender) {
            if (sender.hasPermission(Bukkit.getPluginManager().getPermission("statsdb.admin"))) {
                sender.sendMessage("Running StatsDB commit " + getDescription().getVersion());
            }
        }

        @Override
        public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
            switch (args.length) {
                case 1:
                    return List.of(Statistic.values())
                            .stream()
                            .map(Enum::name)
                            .filter(name -> name.startsWith(args[0]))
                            .collect(Collectors.toList());
                case 2:
                    Type statType = Statistic.valueOf(args[0]).getType();
                    switch (statType) {
                        case UNTYPED:
                            return Bukkit.getOnlinePlayers()
                                    .stream()
                                    .map(Player::getDisplayName)
                                    .filter(name -> name.startsWith(args[1]))
                                    .collect(Collectors.toList());
                        case BLOCK:
                            return List.of(Material.values())
                                    .stream()
                                    .filter(Material::isBlock)
                                    .map(Enum::name)
                                    .filter(name -> name.startsWith(args[1]))
                                    .collect(Collectors.toList());
                        case ITEM:
                            return List.of(Material.values())
                                    .stream()
                                    .filter(material -> !material.isBlock())
                                    .map(Enum::name)
                                    .filter(name -> name.startsWith(args[1]))
                                    .collect(Collectors.toList());
                        case ENTITY:
                            return List.of(EntityType.values())
                                    .stream()
                                    .map(Enum::name)
                                    .filter(name -> name.startsWith(args[1]))
                                    .collect(Collectors.toList());
                    }
                case 3:
                    return Bukkit.getOnlinePlayers()
                            .stream()
                            .map(Player::getDisplayName)
                            .filter(name -> name.startsWith(args[2]))
                            .collect(Collectors.toList());
            }
            return null;
        }
    }
}
