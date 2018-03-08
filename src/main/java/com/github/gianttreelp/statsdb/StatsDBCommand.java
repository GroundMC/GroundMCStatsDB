package com.github.gianttreelp.statsdb;

import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.OfflinePlayer;
import org.bukkit.Statistic;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
class StatsDBCommand implements CommandExecutor, TabCompleter {

    final private StatsDB statsDB;

    StatsDBCommand(StatsDB statsDB) {
        this.statsDB = statsDB;
    }

    private boolean startsWithIgnoreCase(String candidate, String prefix) {
        return candidate.toLowerCase(Locale.ROOT).startsWith(prefix.toLowerCase(Locale.ROOT));
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        Bukkit.getScheduler().runTaskAsynchronously(statsDB, () -> {
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
            Connection connection = StatsDB.getConnection();
            if (sender instanceof Player) {
                //noinspection deprecation
                OfflinePlayer player = Bukkit.getOfflinePlayer(args[2]);

                appendSubstatisticSQL(stat, builder);

                PreparedStatement statement = connection.prepareStatement(
                        builder.toString());
                statement.setBytes(1, StatsDB.getBytesFromUUID(player));
                statement.setString(2, stat.name());
                statement.setString(3, args[1]);
                executeSendAndClose(sender, stat, statement);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void executeSendAndClose(CommandSender sender, Statistic stat, PreparedStatement statement) throws SQLException {
        ResultSet result = statement.executeQuery();
        if (result.next()) {
            sender.sendMessage(stat.name() + ": " + result.getString(1));
        }
        result.close();
        statement.close();
    }

    private void subStatisticOrPlayerStatistic(CommandSender sender, String[] args) {
        try {
            Statistic stat = Statistic.valueOf(args[0]);
            StringBuilder builder = new StringBuilder(
                    "SELECT `value` FROM `Statistics` " +
                            "WHERE `player_id` = ? " +
                            "AND `statistic` = ? ");
            Connection connection = StatsDB.getConnection();
            if (stat.isSubstatistic()) {
                if (sender instanceof Player) {
                    Player player = (Player) sender;

                    appendSubstatisticSQL(stat, builder);

                    PreparedStatement statement = connection.prepareStatement(
                            builder.toString());
                    statement.setBytes(1, StatsDB.getBytesFromUUID(player));
                    statement.setString(2, stat.name());
                    statement.setString(3, args[1]);
                    executeSendAndClose(sender, stat, statement);
                }
            } else {
                //noinspection deprecation
                OfflinePlayer player = Bukkit.getOfflinePlayer(args[1]);
                PreparedStatement statement = connection.prepareStatement(
                        builder.toString());
                statement.setBytes(1, StatsDB.getBytesFromUUID(player));
                statement.setString(2, stat.name());
                ResultSet result = statement.executeQuery();
                if (result.next()) {
                    sender.sendMessage(stat.name() + ": " + result.getString(1));
                } else {
                    sender.sendMessage("Statistic not recorded yet.");
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
                Connection connection = StatsDB.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT `value` FROM `Statistics` " +
                                "WHERE `player_id` = ?" +
                                "AND `statistic` = ?"
                );
                statement.setBytes(1, StatsDB.getBytesFromUUID(player));
                statement.setString(2, stat.name());
                executeSendAndClose(sender, stat, statement);
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
            sender.sendMessage("Running StatsDB commit " + statsDB.getDescription().getVersion());
        }
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        switch (args.length) {
            case 1:
                return getStatisticNameBeginningWith(args[0]);
            case 2:
                switch (Statistic.valueOf(args[0]).getType()) {
                    case UNTYPED:
                        return getPlayerNamesBeginningWith(args[1]);
                    case BLOCK:
                        return getBlockNamesBeginningWith(args[1]);
                    case ITEM:
                        return getItemNamesBeginningWith(args[1]);
                    case ENTITY:
                        return getEntityNamesBeginningWith(args[1]);
                }
            case 3:
                return getPlayerNamesBeginningWith(args[2]);
        }
        return null;
    }

    private List<String> getEntityNamesBeginningWith(String begin) {
        List<String> list = new ArrayList<>(EntityType.values().length / 3);
        for (EntityType entityType : EntityType.values()) {
            String name = entityType.name();
            if (startsWithIgnoreCase(name, begin)) {
                list.add(name);
            }
        }
        return list;
    }

    private List<String> getItemNamesBeginningWith(String beign) {
        List<String> list = new ArrayList<>();
        for (Material material : Material.values()) {
            if (!material.isBlock()) {
                String name = material.name();
                if (startsWithIgnoreCase(name, beign)) {
                    list.add(name);
                }
            }
        }
        return list;
    }

    private List<String> getStatisticNameBeginningWith(String begin) {
        List<String> list = new ArrayList<>(Statistic.values().length / 3);
        for (Statistic statistic : Statistic.values()) {
            String name = statistic.name();
            if (startsWithIgnoreCase(name, begin)) {
                list.add(name);
            }
        }
        return list;
    }

    private List<String> getBlockNamesBeginningWith(String begin) {
        List<String> list = new ArrayList<>();
        for (Material material : Material.values()) {
            if (material.isBlock()) {
                String name = material.name();
                if (startsWithIgnoreCase(name, begin)) {
                    list.add(name);
                }
            }
        }
        return list;
    }

    private List<String> getPlayerNamesBeginningWith(String begin) {
        List<String> list = new ArrayList<>(Bukkit.getOnlinePlayers().size() / 3);
        for (Player player : Bukkit.getOnlinePlayers()) {
            String name = player.getDisplayName();
            if (startsWithIgnoreCase(name, begin)) {
                list.add(name);
            }
        }
        return list;
    }
}
