package com.github.gianttreelp.statsdb;

import com.google.common.collect.Queues;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerStatisticIncrementEvent;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.UUID;

public class EventListener implements Listener {

    public Queue<PreparedStatement> statements = Queues.newConcurrentLinkedQueue();
    public PreparedStatement insertStatement;

    {
        try {
            insertStatement = StatsDB.getConnection().prepareStatement("INSERT INTO `Statistics`(" +
                    "`server_id`, `player_id`, `statistic`, `value`, `material`, `entity`) " +
                    "VALUES (?, ?, ?, ?, ?, ?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @EventHandler
    public void onStatisticIncrement(PlayerStatisticIncrementEvent event) {

        Connection connection = StatsDB.getConnection();
        String serverIdentifier = StatsDB.getConfiguration().getString("server.identifier");
        UUID playerId = event.getPlayer().getUniqueId();
        byte[] uuid = new byte[16];
        ByteBuffer.wrap(uuid).order(ByteOrder.BIG_ENDIAN)
                .putLong(playerId.getMostSignificantBits())
                .putLong(playerId.getLeastSignificantBits());
        try {
            StringBuilder builder = new StringBuilder()
                    .append("DELETE FROM `Statistics` WHERE ")
                    .append("`server_id` = ? ")
                    .append("AND `player_id` = ? ")
                    .append("AND `statistic` = ? ");

            if (event.getMaterial() != null) {
                builder.append("AND `material` = ? ");
            }
            if (event.getEntityType() != null) {
                builder.append("AND `entity` = ? ");
            }
            PreparedStatement statement = connection.prepareStatement(builder.toString());
            statement.setString(1, serverIdentifier);
            statement.setBytes(2, uuid);
            statement.setString(3, event.getStatistic().name());
            if (event.getMaterial() != null) {
                statement.setString(4, event.getMaterial().name());
            } else if (event.getEntityType() != null) {
                statement.setString(4, event.getEntityType().name());
            }
            statements.add(statement);

            insertStatement.setString(1, serverIdentifier);
            insertStatement.setBytes(2, uuid);
            insertStatement.setString(3, event.getStatistic().name());
            insertStatement.setInt(4, event.getNewValue());
            if (event.getMaterial() != null) {
                insertStatement.setString(5, event.getMaterial().name());
            } else if (event.getEntityType() != null) {
                insertStatement.setString(5, event.getEntityType().name());
            }
            insertStatement.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
