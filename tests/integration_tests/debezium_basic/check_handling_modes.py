import base64
import json
import sys


path, mode = sys.argv[1:]
decimal = "12345678901234567890123456789012345.123456789012345678901234567890"


def binary(value):
    if mode == "hex":
        return value.hex()
    if mode == "base64-url-safe":
        return base64.urlsafe_b64encode(value).decode()
    return base64.b64encode(value).decode()


def row(key):
    return {
        "id": binary(key),
        "signed_value": 9007199254740993,
        "unsigned_value": "18446744073709551615",
        "decimal_value": decimal,
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
               unsigned_value="9223372036854775808", decimal_value="-" + decimal,
               blob_value=binary(b"\x00\xff"))
expected[("u", binary(b"\x01"))] = (row(b"\x01"), updated)
expected[("d", binary(b"\x03"))] = (row(b"\x03"), None)

seen = set()
with open(path) as messages:
    for line in messages:
        message = json.loads(line)
        payload = message.get("payload") or {}
        if payload.get("source", {}).get("table") != "handling_modes" or "op" not in payload:
            continue
        before, after = payload["before"], payload["after"]
        event = (payload["op"], (after or before)["id"])
        assert event in expected, (mode, "unexpected event", event)
        assert (before, after) == expected[event], (mode, event, before, after)
        seen.add(event)

        # Check both row schemas, including exact defaults. json.loads keeps
        # signed BIGINT values as Python integers without float64 rounding.
        envelope = {field["field"]: field for field in message["schema"]["fields"]}
        for image in ["before", "after"]:
            fields = {field["field"]: field for field in envelope[image]["fields"]}
            assert fields["signed_value"]["type"] == "int64", fields
            assert fields["signed_value"]["default"] == 9007199254740993, fields
            assert fields["unsigned_value"]["type"] == "string", fields
            assert fields["unsigned_value"]["default"] == "18446744073709551615", fields
            assert fields["decimal_value"]["type"] == "string", fields
            assert fields["decimal_value"]["default"] == decimal, fields
            assert fields["null_unsigned"]["type"] == "string", fields
            assert fields["null_decimal"]["type"] == "string", fields
            for name in ["id", "fixed_value", "blob_value", "empty_value", "null_value", "default_value"]:
                assert fields[name]["type"] == ("bytes" if mode == "bytes" else "string"), fields[name]
            assert fields["default_value"]["default"] == binary(b"ab"), fields
            assert fields["text_value"]["type"] == "string", fields
            assert fields["bit_value"]["type"] == "bytes", fields
            assert fields["bit_value"]["default"] == "BQAAAAAAAAA=", fields

assert seen == expected.keys(), (mode, "missing events", expected.keys() - seen)
print(f"{mode}: verified {len(seen)} Debezium row events and their schemas")
