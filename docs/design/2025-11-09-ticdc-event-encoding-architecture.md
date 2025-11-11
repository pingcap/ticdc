# Event Encoding Architecture Design

- Author(s): [dongmen](https://github.com/asddongmen)
- Tracking Issue(s): [2940](https://github.com/pingcap/ticdc/issues/2940)

## Overview

This document describes the unified event encoding/decoding architecture for all event types in TiCDC. The new architecture introduces a standardized wire format with a unified header, enabling forward and backward compatibility for future protocol evolution.

## Motivation

### Problems with Previous Architecture

The previous event encoding architecture had several issues:

1. **Inconsistent Format**: Each event type had its own encoding format without a unified standard
2. **No Protocol Validation**: No magic bytes to validate data integrity
3. **No Type Identification**: Event type was not embedded in the serialized data
4. **Version in Payload**: Version byte was part of the payload, making header parsing inconsistent
5. **No Forward Compatibility**: Impossible to gracefully handle unknown versions or event types

### Goals

1. **Unified Wire Format**: All events share the same header structure
2. **Forward Compatibility**: New versions can be added without breaking old clients
3. **Backward Compatibility**: Old data can still be parsed by new code (within the same generation)
4. **Type Safety**: Clear event type identification in serialized format
5. **Efficient Parsing**: Fixed-size header enables fast pre-parsing

## Architecture

### Wire Format

Every serialized event now follows this unified format:

```
┌──────────────────────────────────────────────────────────────────┐
│                        Serialized Event                          │
├─────────────────────────────┬────────────────────────────────────┤
│      Header (16 bytes)       │       Payload (Variable)           │
├──────┬──────┬──────┬────────┼────────────────────────────────────┤
│MAGIC │ TYPE │ VER  │ LENGTH │    Event-specific Data             │
│ 4B   │  2B  │  2B  │   8B   │    (Business Fields)               │
└──────┴──────┴──────┴────────┴────────────────────────────────────┘
  0xDA7A6A6A   0-65535   0-65535   uint64      (DispatcherID, Seq, Epoch, etc.)
```

### Header Layout

The header is **16 bytes** fixed size with the following fields:

| Field          | Size | Type    | Description                                    |
|----------------|------|---------|------------------------------------------------|
| Magic Bytes    | 4B   | bytes   | `0xDA 0x7A 0x6A 0x6A` - Format validation marker |
| Event Type     | 2B   | uint16  | Event type identifier                          |
| Version        | 2B   | uint16  | Payload format version                         |
| Payload Length | 8B   | uint64  | Length of payload in bytes (big-endian)        |

### Key Design Principles

#### 1. **Header vs Payload Separation**

The header is purely for **wire format metadata** and does not contain business logic:
- Header: Protocol-level information (magic, type, version, length)
- Payload: Business data (DispatcherID, Seq, Epoch, timestamps, etc.)

#### 2. **GetSize() Returns Business Data Size Only**

```go
func (e *DropEvent) GetSize() int64 {
    // Size does NOT include header or version (those are only for serialization)
    // Only business data: dispatcherID + seq + commitTs + epoch
    return int64(e.DispatcherID.GetSize() + 8 + 8 + 8)
}
```

**Rationale**: 
- The header is a serialization concern, not a memory concern
- `GetSize()` is used for memory accounting and monitoring
- Serialized size = `GetSize()` + `GetEventHeaderSize()`

#### 3. **Version is Moved to Header**

**Before** (Old Architecture):
```
┌──────────────────────────────────────┐
│         Payload                      │
├──────┬───────────────────────────────┤
│ VER  │  Business Data                │
│  1B  │  (DispatcherID, Seq, ...)     │
└──────┴───────────────────────────────┘
```

**After** (New Architecture):
```
┌────────────────┬───────────────────────┐
│    Header      │      Payload          │
├──────┬─────────┼───────────────────────┤
│MAGIC │TYPE│VER │  Business Data        │
│ 2B   │ 1B │ 1B │  (DispatcherID, ...)  │
└──────┴────┴────┴───────────────────────┘
```

## Event Types

Current supported event types:

| Type ID | Event Name                | Version | Status      |
|---------|---------------------------|---------|-------------|
| 0       | DMLEvent                  | 0       | ✅ Migrated |
| 1       | BatchDMLEvent             | 0       | ✅ Migrated |
| 2       | DDLEvent                  | 0       | ✅ Migrated |
| 3       | ResolvedEvent             | 0       | ✅ Migrated |
| 4       | BatchResolvedEvent        | 0       | ✅ Migrated |
| 5       | SyncPointEvent            | 0       | ✅ Migrated |
| 6       | ReadyEvent                | 0       | ✅ Migrated |
| 7       | HandshakeEvent            | 0       | ✅ Migrated |
| 8       | NotReusableEvent          | 0       | ✅ Migrated |
| 9       | DropEvent                 | 0       | ✅ Migrated |
| 10      | CongestionControl         | 0       | ✅ Migrated |
| 11      | DispatcherHeartbeat       | 0       | ✅ Migrated |
| 12      | DispatcherHeartbeatResponse | 0    | ✅ Migrated |


### Scenario: Add Version 1 for DropEvent

Suppose we want to add a new field `DroppedReason string` to `DropEvent`:

#### Step 1: Define the New Version Constant

```go
const (
    DropEventVersion1 = 1
    DropEventVersion2 = 2  // New version
)
```

#### Step 2: Add New Field to Struct

```go
type DropEvent struct {
    Version         byte
    DispatcherID    common.DispatcherID
    DroppedSeq      uint64
    DroppedCommitTs common.Ts
    DroppedEpoch    uint64
    DroppedReason   string  // New field (V1 only)
}
```

#### Step 3: Update GetSize()

```go
func (e *DropEvent) GetSize() int64 {
    size := int64(e.DispatcherID.GetSize() + 8 + 8 + 8)
    
    // Add size for V1 fields
    if e.Version >= DropEventVersion1 {
        size += int64(4 + len(e.DroppedReason))  // 4 bytes for length + string content
    }
    
    return size
}
```

#### Step 4: Implement encodeV1() and decodeV1()

```go
func (e DropEvent) encodeV1() ([]byte, error) {
    // V1 includes all V0 fields + new fields
    payloadSize := 8 + 8 + 8 + e.DispatcherID.GetSize() + 4 + len(e.DroppedReason)
    data := make([]byte, payloadSize)
    offset := 0

    // V0 fields
    copy(data[offset:], e.DispatcherID.Marshal())
    offset += e.DispatcherID.GetSize()
    binary.BigEndian.PutUint64(data[offset:], e.DroppedSeq)
    offset += 8
    binary.BigEndian.PutUint64(data[offset:], uint64(e.DroppedCommitTs))
    offset += 8
    binary.BigEndian.PutUint64(data[offset:], e.DroppedEpoch)
    offset += 8

    // V1 new fields
    binary.BigEndian.PutUint32(data[offset:], uint32(len(e.DroppedReason)))
    offset += 4
    copy(data[offset:], []byte(e.DroppedReason))

    return data, nil
}

func (e *DropEvent) decodeV1(data []byte) error {
    offset := 0

    // V0 fields
    dispatcherIDSize := e.DispatcherID.GetSize()
    err := e.DispatcherID.Unmarshal(data[offset : offset+dispatcherIDSize])
    if err != nil {
        return err
    }
    offset += dispatcherIDSize
    e.DroppedSeq = binary.BigEndian.Uint64(data[offset:])
    offset += 8
    e.DroppedCommitTs = common.Ts(binary.BigEndian.Uint64(data[offset:]))
    offset += 8
    e.DroppedEpoch = binary.BigEndian.Uint64(data[offset:])
    offset += 8

    // V1 new fields
    reasonLen := binary.BigEndian.Uint32(data[offset:])
    offset += 4
    e.DroppedReason = string(data[offset : offset+int(reasonLen)])

    return nil
}
```

#### Step 5: Update Marshal/Unmarshal Switch Statements

```go
func (e DropEvent) Marshal() ([]byte, error) {
    var payload []byte
    var err error
    switch e.Version {
    case DropEventVersion1:
        payload, err = e.encodeV1()
        if err != nil {
            return nil, err
        }
    case DropEventVersion2:  // New case
        payload, err = e.encodeV2()
        if err != nil {
            return nil, err
        }
    default:
        return nil, fmt.Errorf("unsupported DropEvent version: %d", e.Version)
    }

    return MarshalEventWithHeader(TypeDropEvent, e.Version, payload)
}

func (e *DropEvent) Unmarshal(data []byte) error {
    // ... header parsing ...

    switch version {
    case DropEventVersion1:
        return e.decodeV1(payload)
    case DropEventVersion2:  // New case
        return e.decodeV2(payload)
    default:
        return fmt.Errorf("unsupported DropEvent version: %d", version)
    }
}
```

#### Step 6: Update Constructor for New Version

```go
func NewDropEventV1(
    dispatcherID common.DispatcherID,
    seq uint64,
    epoch uint64,
    commitTs common.Ts,
    reason string,  // New parameter
) *DropEvent {
    return &DropEvent{
        Version:         DropEventVersion1,  // Use V1
        DispatcherID:    dispatcherID,
        DroppedSeq:      seq,
        DroppedCommitTs: commitTs,
        DroppedEpoch:    epoch,
        DroppedReason:   reason,
    }
}
```

#### Step 7: Add Tests

```go
func TestDropEventV1(t *testing.T) {
    did := common.NewDispatcherID()
    e := NewDropEventV1(did, 123, 100, 456, "memory pressure")
    
    data, err := e.Marshal()
    require.NoError(t, err)
    require.Len(t, data, int(e.GetSize())+GetEventHeaderSize())

    var e2 DropEvent
    err = e2.Unmarshal(data)
    require.NoError(t, err)
    require.Equal(t, e.Version, e2.Version)
    require.Equal(t, e.DroppedReason, e2.DroppedReason)
}
```

## Compatibility

### Breaking Change with Old Format

⚠️ **This PR introduces a BREAKING CHANGE** ⚠️

The new architecture is **NOT compatible** with the previous encoding format. This is a one-time breaking change to establish a solid foundation for future compatibility.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Compatibility Timeline                           │
└─────────────────────────────────────────────────────────────────────────┘

                    THIS PR
                       ↓
    ═══════════════════╪═══════════════════════════════════════════════
    Old Format         │         New Format (With Header)
    (No Header)        │
    ═══════════════════╪═══════════════════════════════════════════════
                       │
                       │
    ┌──────────────┐   │   ┌──────────────┐   ┌──────────────┐
    │   V0 (Old)   │   │   │  V0 (New)    │   │     V1       │
    │   No Magic   │   │   │  + Header    │   │  + Header    │
    │   No Type    │   │   │  + Magic     │   │  + Magic     │
    │   Ver in     │   │   │  + Type      │   │  + Type      │
    │   Payload    │   │   │  + Ver       │   │  + Ver       │
    └──────────────┘   │   └──────────────┘   └──────────────┘
                       │           │                   │
         ✗             │           │                   │
    NOT COMPATIBLE     │           ├───────────────────┤
         WITH          │           │   ✓ COMPATIBLE     │
    NEW FORMAT        │           │   Forward/Backward │
                       │           └───────────────────┘
                       │
    ═══════════════════╪═══════════════════════════════════════════════
    Before PR          │  After PR (Future-proof)
    ═══════════════════╪═══════════════════════════════════════════════
```

### Forward and Backward Compatibility (After This PR)

Once this PR is merged, all future versions **within the same event type** will be compatible:

```
┌────────────────────────────────────────────────────────────────────┐
│             Version Compatibility Matrix (After PR)                │
└────────────────────────────────────────────────────────────────────┘

    Reader Version
         ↓
    ┌─────┬────────┬────────┬────────┬─────────┐
    │     │  V0    │  V1    │  V2    │  V3     │  ← Writer Version
    ├─────┼────────┼────────┼────────┼─────────┤
    │ V0  │   ✓    │   ✗    │   ✗    │   ✗     │
    │     │  R/W   │  Can't │  Can't │  Can't  │
    │     │        │  Parse │  Parse │  Parse  │
    ├─────┼────────┼────────┼────────┼─────────┤
    │ V1  │   ✓    │   ✓    │   ✗    │   ✗     │
    │     │  Read  │  R/W   │  Can't │  Can't  │
    │     │  V0    │        │  Parse │  Parse  │
    ├─────┼────────┼────────┼────────┼─────────┤
    │ V2  │   ✓    │   ✓    │   ✓    │   ✗     │
    │     │  Read  │  Read  │  R/W   │  Can't  │
    │     │  V0    │  V1    │        │  Parse  │
    ├─────┼────────┼────────┼────────┼─────────┤
    │ V3  │   ✓    │   ✓    │   ✓    │   ✓     │
    │     │  Read  │  Read  │  Read  │  R/W    │
    │     │  V0    │  V1    │  V2    │         │
    └─────┴────────┴────────┴────────┴─────────┘

    Legend:
    ✓ = Compatible
    ✗ = Incompatible
    R/W = Read and Write
```

**Compatibility Rules**:
1. **Backward Compatible**: New code can read old data (same or lower version)
2. **Forward Incompatible**: Old code cannot read new data (higher version)
3. **Same Version**: Full read/write compatibility

### Version Negotiation Flow

```
┌──────────────┐                              ┌──────────────┐
│   Sender     │                              │   Receiver   │
│  (V2 Code)   │                              │  (V1 Code)   │
└──────┬───────┘                              └──────┬───────┘
       │                                             │
       │  1. Marshal Event with V2 Format           │
       │     [Header: Type=3, Ver=2, Len=100]       │
       │     + V2 Payload                           │
       ├────────────────────────────────────────────>│
       │                                             │
       │                                             │ 2. UnmarshalEventHeader()
       │                                             │    - Read Header: Ver=2
       │                                             │
       │                                             │ 3. Version Check
       │                                             │    switch version {
       │                                             │      case V0: ✓
       │                                             │      case V1: ✓
       │                                             │      case V2: ✗ (Not Supported)
       │                                             │    }
       │                                             │
       │  4. Error: "unsupported version: 2"        │
       │<────────────────────────────────────────────┤
       │                                             │
       │  5. Sender Fallback to V1                  │
       │     [Header: Type=3, Ver=1, Len=80]        │
       │     + V1 Payload                           │
       ├────────────────────────────────────────────>│
       │                                             │
       │                                             │ 6. Decode V1 Successfully ✓
       │                                             │
```

## Migration Strategy

### For This PR (One-time Migration)

1. **Stop all TiCDC instances** - This is a breaking change
2. **Deploy new version** with unified header format
3. **Restart all instances** - Old data in transit will be lost
4. **Monitor logs** for any parsing errors

### For Future Version Changes (After This PR)

1. **Deploy new version gradually** - Rolling upgrade is safe
2. **New instances** can read both old and new versions
3. **Old instances** will reject newer versions gracefully

## Testing

### Test Coverage

Each event type should have:

1. **Basic Marshal/Unmarshal Test**
   ```go
   func TestEventMarshalUnmarshal(t *testing.T)
   ```

2. **Header Validation Test**
   ```go
   func TestEventHeader(t *testing.T)
   ```

3. **Error Handling Test**
   ```go
   func TestEventUnmarshalErrors(t *testing.T)
   ```

4. **Size Calculation Test**
   ```go
   func TestEventSize(t *testing.T)
   ```

5. **Version-specific Tests** (when multiple versions exist)
   ```go
   func TestEventV0(t *testing.T)
   func TestEventV1(t *testing.T)
   func TestEventV0ToV1Compatibility(t *testing.T)
   ```

### Integration Tests

```go
func TestCrossVersionCompatibility(t *testing.T) {
    // Test that V1 code can read V0 data
    v0Event := createV0Event()
    v0Data := marshalWithV0Code(v0Event)
    
    var v1Event EventType
    err := v1Event.Unmarshal(v0Data)  // V1 Unmarshal reads V0 data
    require.NoError(t, err)
}
```

## Best Practices

### DO

✅ Always use `MarshalEventWithHeader()` for encoding  
✅ Always use `UnmarshalEventHeader()` for header parsing  
✅ Keep `GetSize()` updated when adding fields  
✅ Add version-specific tests for new versions  
✅ Document breaking changes in version changelog  
✅ Use meaningful version numbers (0, 1, 2, ...)  
✅ Validate header magic bytes and event type  

### DON'T

❌ Don't include header/version in `GetSize()` calculation  
❌ Don't encode version in payload (it's in the header)  
❌ Don't skip version validation in Unmarshal  
❌ Don't reuse version numbers for different formats  
❌ Don't add version-breaking changes without incrementing version  
❌ Don't panic on unknown versions (return error instead)  
