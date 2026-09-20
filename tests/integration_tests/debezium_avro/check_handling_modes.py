import base64
import json
import sys
from fractions import Fraction


path, mode = sys.argv[1:]
decimal = "12345678901234567890123456789012345.123456789012345678901234567890"


def binary(value):
    # kafka_dump renders decoded Avro bytes as Base64 in its JSON output.
    return base64.b64encode(value).decode()


def decimal_value(value):
    return value if mode == "string" else str(Fraction(value))


def unwrap(value):
    if isinstance(value, dict):
        if len(value) == 1:
            branch = next(iter(value))
            if branch in ["long", "string", "bytes"] or "." in branch:
                return unwrap(value[branch])
        return {key: unwrap(item) for key, item in value.items()}
    return value


def field_type(field):
    field = field["type"]
    return next(branch for branch in field if branch != "null") if isinstance(field, list) else field


def row(key):
    return {
        "id": binary(key),
        "signed_value": 9007199254740993,
        "unsigned_value": 9223372036854775807 if mode == "default" else "18446744073709551615",
        "decimal_value": decimal_value(decimal),
        "null_unsigned": None,
        "null_decimal": None,
        "fixed_value": binary(b"\xfb\xff\x00\x00"),
        "blob_value": binary(b"\xfb\xff"),
        "empty_value": "",
        "null_value": None,
        "default_value": binary(b"ab"),
        "text_value": "plain",
        "bit_value": "BQAAAAAAAAA=",
    }


expected = {}
for key in [b"\xfb\xff", b"\x01", b"\x03"]:
    expected[("c", binary(key))] = (None, row(key))
null_row = row(b"\x02")
for field in ["fixed_value", "blob_value", "empty_value"]:
    null_row[field] = None
expected[("c", binary(b"\x02"))] = (None, null_row)
updated = dict(row(b"\x01"), signed_value=9223372036854775807,
               unsigned_value=9007199254740993 if mode == "default" else "9223372036854775808",
               decimal_value=decimal_value("-" + decimal), blob_value=binary(b"\x00\xff"))
expected[("u", binary(b"\x01"))] = (row(b"\x01"), updated)
expected[("d", binary(b"\x03"))] = (row(b"\x03"), None)

seen = set()
with open(path) as messages:
    for line in messages:
        message = json.loads(line)
        payload = unwrap(message["payload"])
        if payload.get("source", {}).get("table") != "handling_modes" or "op" not in payload:
            continue
        before, after = payload["before"], payload["after"]
        event = (payload["op"], (after or before)["id"])
        assert event in expected, (mode, "unexpected event", event)
        assert (before, after) == expected[event], (mode, event, before, after)
        assert unwrap(message["key"]["payload"]) == {"id": event[1]}, message["key"]
        seen.add(event)

        # Inspect the schema fetched by the Confluent header's schema ID.
        key_fields = {field["name"]: field for field in message["key"]["schema"]["fields"]}
        assert field_type(key_fields["id"]) == "bytes", key_fields
        envelope = {field["name"]: field for field in message["schema"]["fields"]}
        row_schema = field_type(envelope["before"])
        assert row_schema["type"] == "record", row_schema
        # The after union refers to the same named row schema.
        after_type = field_type(envelope["after"])
        assert after_type in [row_schema["name"], row_schema.get("namespace", "") + "." + row_schema["name"]], after_type
        fields = {field["name"]: field for field in row_schema["fields"]}
        assert field_type(fields["signed_value"]) == "long", fields
        for name in ["unsigned_value", "null_unsigned"]:
            assert field_type(fields[name]) == ("long" if mode == "default" else "string"), fields[name]
        for name in ["decimal_value", "null_decimal"]:
            decimal_schema = field_type(fields[name])
            if mode == "string":
                assert decimal_schema == "string", decimal_schema
            else:
                assert decimal_schema["type"] == "bytes", decimal_schema
                assert decimal_schema["logicalType"] == "decimal", decimal_schema
                assert decimal_schema["precision"] == 65, decimal_schema
                assert decimal_schema["scale"] == 30, decimal_schema
        for name in ["id", "fixed_value", "blob_value", "empty_value", "null_value", "default_value"]:
            assert field_type(fields[name]) == "bytes", fields[name]
        bit_schema = field_type(fields["bit_value"])
        assert bit_schema["type"] == "bytes", bit_schema
        assert bit_schema["connect.name"] == "io.debezium.data.Bits", bit_schema
        assert bit_schema["connect.parameters"]["length"] == "64", bit_schema
        assert field_type(fields["text_value"]) == "string", fields

assert seen == expected.keys(), (mode, "missing events", expected.keys() - seen)
print(f"{mode}: verified {len(seen)} Debezium Avro row events, keys and schemas")
