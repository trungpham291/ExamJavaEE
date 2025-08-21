package com.example.dao;

import com.example.model.PlayerIndex;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class PlayerIndexDAO {
    private final MongoCollection<Document> col;
    private final IndexerDAO indexerDAO;

    public PlayerIndexDAO(MongoDatabase db, IndexerDAO indexerDAO) {
        this.col = db.getCollection("player_index");
        this.indexerDAO = indexerDAO;
    }

    private static PlayerIndex toEntity(Document d) {
        if (d == null) return null;
        return new PlayerIndex(
                d.getObjectId("_id"),
                d.getObjectId("player_id"),
                d.getObjectId("index_id"),
                d.getDouble("value")
        );
    }

    private static Document toDoc(PlayerIndex pi) {
        Document d = new Document("player_id", pi.getPlayerId())
                .append("index_id", pi.getIndexId())
                .append("value", pi.getValue());
        if (pi.getId() != null) d.put("_id", pi.getId());
        return d;
    }

    // ======= READ =======

    public List<PlayerIndex> listByPlayer(ObjectId playerId) {
        List<PlayerIndex> list = new ArrayList<>();
        try (MongoCursor<Document> cur = col.find(Filters.eq("player_id", playerId)).iterator()) {
            while (cur.hasNext()) list.add(toEntity(cur.next()));
        }
        return list;
    }

    public PlayerIndex findById(ObjectId id) {
        return toEntity(col.find(Filters.eq("_id", id)).first());
    }

    // ======= CREATE / UPDATE with VALIDATE =======

    /** insert mới nếu id == null; ngược lại update */
    public ObjectId upsert(PlayerIndex pi) {
        if (!indexerDAO.isWithinRange(pi.getIndexId(), pi.getValue()))
            throw new IllegalArgumentException("Giá trị ngoài khoảng cho indexer này");

        if (pi.getId() == null) {
            Document d = toDoc(pi);
            col.insertOne(d);
            return d.getObjectId("_id");
        } else {
            col.updateOne(Filters.eq("_id", pi.getId()),
                    new Document("$set", toDoc(pi)));
            return pi.getId();
        }
    }

    // ======= DELETE =======

    public void deleteById(ObjectId id) {
        col.deleteOne(Filters.eq("_id", id));
    }

    public void deleteAllOfPlayer(ObjectId playerId) {
        col.deleteMany(Filters.eq("player_id", playerId));
    }
    // === JOIN player_index + player + indexer để in bảng phẳng
    public java.util.List<org.bson.Document> listAllJoined() {
        java.util.List<org.bson.Document> out = new java.util.ArrayList<>();
        java.util.List<org.bson.Document> pipeline = java.util.List.of(
                new org.bson.Document("$lookup",
                        new org.bson.Document("from", "player")
                                .append("localField", "player_id")
                                .append("foreignField", "_id")
                                .append("as", "player")),
                new org.bson.Document("$unwind", "$player"),
                new org.bson.Document("$lookup",
                        new org.bson.Document("from", "indexer")
                                .append("localField", "index_id")
                                .append("foreignField", "_id")
                                .append("as", "indexer")),
                new org.bson.Document("$unwind", "$indexer"),
                new org.bson.Document("$project",
                        new org.bson.Document("_id", 1) // _id của player_index
                                .append("player_id", "$player._id")
                                .append("player_name", "$player.name")
                                .append("player_age", "$player.age")
                                .append("index_name", "$indexer.name")
                                .append("value", "$value")),
                new org.bson.Document("$sort",
                        new org.bson.Document("player_name", 1).append("index_name", 1))
        );
        try (com.mongodb.client.MongoCursor<org.bson.Document> cur =
                     col.aggregate(pipeline).iterator()) {
            while (cur.hasNext()) out.add(cur.next());
        }
        return out;
    }

}
