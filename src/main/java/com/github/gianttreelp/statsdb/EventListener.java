package com.github.gianttreelp.statsdb;

import org.apache.commons.lang.StringUtils;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerStatisticIncrementEvent;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class EventListener implements Listener {
    @EventHandler
    public void a(PlayerStatisticIncrementEvent event) {

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
                    .append("AND `player_id` = ?")
                    .append("AND `statistic` = ?");

            if (event.getMaterial() != null) {
                builder.append("AND `material` = ?");
            }
            if (event.getEntityType() != null) {
                builder.append("AND `entity` = ?");
            }
            PreparedStatement statement = connection.prepareStatement(builder.toString());
            statement.setString(1, serverIdentifier);
            statement.setBytes(2, uuid);
            statement.setString(3, event.getStatistic().name());
            if (event.getMaterial() != null) {
                statement.setString(4, event.getMaterial().name());
            }
            if (event.getEntityType() != null) {
                statement.setString(5, event.getEntityType().name());
            }
            statement.execute();

            int columns = 4;
            builder = new StringBuilder().append("INSERT INTO `Statistics`(")
                    .append("`server_id`, `player_id`, `statistic`, `value`");
            if (event.getMaterial() != null) {
                builder.append(", `material`");
                columns++;
            }
            if (event.getEntityType() != null) {
                builder.append(", `entity`");
                columns++;
            }
            builder.append(" ) VALUES (");
            builder.append(StringUtils.repeat("?", ", ", columns));
            builder.append(")");

            statement = connection.prepareStatement(builder.toString());
            statement.setString(1, serverIdentifier);
            statement.setBytes(2, uuid);
            statement.setString(3, event.getStatistic().name());
            statement.setInt(4, event.getNewValue());
            if (event.getMaterial() != null) {
                statement.setString(5, event.getMaterial().name());
            }
            if (event.getEntityType() != null) {
                statement.setString(6, event.getEntityType().name());
            }
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
