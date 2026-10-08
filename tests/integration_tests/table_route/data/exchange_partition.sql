ALTER TABLE source_db.exchange_partitioned
    EXCHANGE PARTITION p0 WITH TABLE source_extra_db.exchange_normal;

-- Both directions must continue using the current source table's route.
INSERT INTO source_db.exchange_partitioned VALUES (3, 'partition_after');
INSERT INTO source_extra_db.exchange_normal VALUES (4, 'normal_after');
