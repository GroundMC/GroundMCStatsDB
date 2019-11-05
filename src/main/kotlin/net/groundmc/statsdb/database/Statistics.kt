package net.groundmc.statsdb.database

import org.bukkit.Material
import org.bukkit.Statistic
import org.bukkit.entity.EntityType
import org.jetbrains.exposed.sql.Table

class Statistics : Table() {
    val uuid = uuid("id").primaryKey()
    val statistic = enumerationByName("statistic", 255, Statistic::class).primaryKey()
    val material = enumerationByName("material", 255, Material::class).nullable().primaryKey()
    val entity = enumerationByName("entity", 255, EntityType::class).primaryKey()
    val value = integer("value")
}
