package net.groundmc.statsdb

import org.bukkit.Material
import org.bukkit.Statistic
import org.bukkit.entity.EntityType
import java.util.*

internal data class StatisticsObject(val uuid: UUID, val statistic: Statistic,
                                     val material: Material?, val entity: EntityType?,
                                     var value: Int) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StatisticsObject

        if (uuid != other.uuid) return false
        if (statistic != other.statistic) return false
        if (material != other.material) return false
        if (entity != other.entity) return false
        if (value != other.value) return false

        return true
    }

    override fun hashCode(): Int {
        var result = uuid.hashCode()
        result = 31 * result + statistic.hashCode()
        if (material != null) {
            result = 31 * result + material.hashCode()
        }
        if (entity != null) {
            result = 31 * result + entity.hashCode()
        }
        result = 31 * result + value
        return result
    }
}
