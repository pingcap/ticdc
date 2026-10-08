CREATE DATABASE debezium_modes;
USE debezium_modes;

CREATE TABLE handling_modes (
    id VARBINARY(4) PRIMARY KEY,
    signed_value BIGINT DEFAULT 9007199254740993,
    unsigned_value BIGINT UNSIGNED DEFAULT 18446744073709551615,
    decimal_value DECIMAL(65,30) DEFAULT 12345678901234567890123456789012345.123456789012345678901234567890,
    null_unsigned BIGINT UNSIGNED,
    null_decimal DECIMAL(65,30),
    fixed_value BINARY(4),
    blob_value BLOB,
    empty_value VARBINARY(4),
    null_value VARBINARY(4),
    default_value VARBINARY(4) DEFAULT 'ab',
    text_value VARCHAR(20) DEFAULT 'plain',
    bit_value BIT(64) DEFAULT b'101'
);

INSERT INTO handling_modes (id, fixed_value, blob_value, empty_value) VALUES
    (x'fbff', x'fbff', x'fbff', x''),
    (x'01', x'fbff', x'fbff', x''),
    (x'03', x'fbff', x'fbff', x'');
INSERT INTO handling_modes (id) VALUES (x'02');
UPDATE handling_modes SET
    signed_value = 9223372036854775807,
    unsigned_value = 9223372036854775808,
    decimal_value = -12345678901234567890123456789012345.123456789012345678901234567890,
    blob_value = x'00ff'
    WHERE id = x'01';
DELETE FROM handling_modes WHERE id = x'03';
