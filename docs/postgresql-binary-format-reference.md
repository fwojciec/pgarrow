# PostgreSQL Binary Format Reference

## Overview

This document provides detailed technical specifications for PostgreSQL's binary COPY format, extracted from official documentation and source code analysis. This information is essential for implementing the PGArrow binary parser.

## Binary COPY Format Structure

### File Header (15 bytes)
```
Bytes 0-10:  "PGCOPY\n\377\r\n\0" (signature)
Bytes 11-14: 32-bit flags field (network byte order)
Bytes 15-18: 32-bit header extension area length (network byte order)
```

**Signature Breakdown:**
- `PGCOPY`: ASCII string "PGCOPY"
- `\n`: Newline (0x0A)
- `\377`: 0xFF byte
- `\r\n`: Carriage return + newline (0x0D 0x0A)
- `\0`: Null byte (0x00)

**Flags Field:**
- Currently only used to indicate if OIDs are included
- Bit 16 (0x00010000): OID inclusion flag
- Other bits reserved for future use

### Tuple Format
Each tuple (row) in the binary format:

```
Bytes 0-1:   16-bit field count (network byte order)
For each field:
  Bytes 0-3:   32-bit field length (network byte order, -1 for NULL)
  Bytes 4-n:   Field data (if length > 0)
```

### File Trailer
```
Bytes 0-1: 16-bit word containing -1 (0xFFFF)
```

## Data Type Binary Encodings

### Built-in Type OIDs and Functions

| OID | Type Name | Send Function | Recv Function | Description |
|-----|-----------|---------------|---------------|-------------|
| 16  | bool      | boolsend      | boolrecv      | Boolean     |
| 20  | int8      | int8send      | int8recv      | 8-byte integer |
| 21  | int2      | int2send      | int2recv      | 2-byte integer |
| 23  | int4      | int4send      | int4recv      | 4-byte integer |
| 25  | text      | textsend      | textrecv      | Variable-length string |
| 700 | float4    | float4send    | float4recv    | Single-precision float |
| 701 | float8    | float8send    | float8recv    | Double-precision float |

### Specific Binary Formats

#### Integer Types (Network Byte Order)

**int2 (smallint) - OID 21:**
- Length: 2 bytes
- Format: Signed 16-bit integer in network (big-endian) byte order
- Range: -32,768 to 32,767
- Example: Value 1000 → `0x03 0xE8`

**int4 (integer) - OID 23:**
- Length: 4 bytes  
- Format: Signed 32-bit integer in network (big-endian) byte order
- Range: -2,147,483,648 to 2,147,483,647
- Example: Value 1000000 → `0x00 0x0F 0x42 0x40`

**int8 (bigint) - OID 20:**
- Length: 8 bytes
- Format: Signed 64-bit integer in network (big-endian) byte order
- Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
- Example: Value 1000000000000 → `0x00 0x00 0x00 0xE8 0xD4 0xA5 0x10 0x00`

#### Floating Point Types (IEEE 754)

**float4 (real) - OID 700:**
- Length: 4 bytes
- Format: IEEE 754 single-precision floating point (binary32)
- Components: 1 sign bit, 8 exponent bits, 23 mantissa bits
- Example: Value 3.14159 → `0x40 0x49 0x0F 0xD0` (approximately)

**float8 (double precision) - OID 701:**
- Length: 8 bytes
- Format: IEEE 754 double-precision floating point (binary64)
- Components: 1 sign bit, 11 exponent bits, 52 mantissa bits
- Example: Value 3.141592653589793 → `0x40 0x09 0x21 0xFB 0x54 0x44 0x2D 0x18`

#### Boolean Type

**bool - OID 16:**
- Length: 1 byte
- Format: Single byte value
- Values: `0x00` = false, `0x01` = true
- Example: true → `0x01`, false → `0x00`

#### Text Type

**text - OID 25:**
- Length: Variable (specified in length field)
- Format: UTF-8 encoded string without null terminator
- Encoding: Must be valid UTF-8
- Example: "Hello" → `0x48 0x65 0x6C 0x6C 0x6F` (5 bytes)

#### NULL Values

**All Types:**
- NULL values are represented by a length field of -1 (0xFFFFFFFF)
- No data bytes follow a NULL length field
- Example: NULL int4 → Length: `0xFF 0xFF 0xFF 0xFF`, Data: (none)

## Implementation Guidelines

### Byte Order Handling
```go
import "encoding/binary"

// Read network byte order (big-endian) integers
func readInt32(data []byte) int32 {
    return int32(binary.BigEndian.Uint32(data))
}

func readInt64(data []byte) int64 {
    return int64(binary.BigEndian.Uint64(data))
}
```

### Error Handling
- Invalid signature: Reject file immediately
- Unexpected EOF: Handle gracefully, may indicate connection issue
- Invalid length field: Validate against remaining data
- Invalid UTF-8 in text: Return encoding error

### Performance Considerations
- Pre-allocate byte slices for known field sizes
- Use unsafe package for zero-copy conversions where appropriate
- Buffer reads to minimize system calls
- Cache type handlers to avoid repeated lookups

## Binary Format Examples

### Example 1: Simple Row with Mixed Types
Query: `SELECT 42::int4, 'Hello'::text, true::bool`

**Binary representation:**
```
Header: PGCOPY\n\377\r\n\0 + flags(0x00000000) + ext_len(0x00000000)
Field Count: 0x0003 (3 fields)

Field 1 (int4):
  Length: 0x00000004 (4 bytes)
  Data:   0x0000002A (42 in network byte order)

Field 2 (text):  
  Length: 0x00000005 (5 bytes)
  Data:   0x48656C6C6F ("Hello" in UTF-8)

Field 3 (bool):
  Length: 0x00000001 (1 byte)  
  Data:   0x01 (true)

Trailer: 0xFFFF
```

### Example 2: Row with NULL Value
Query: `SELECT 100::int4, NULL::text, false::bool`

**Binary representation:**
```
Header: (same as above)
Field Count: 0x0003 (3 fields)

Field 1 (int4):
  Length: 0x00000004 (4 bytes)
  Data:   0x00000064 (100 in network byte order)

Field 2 (text):
  Length: 0xFFFFFFFF (-1, indicates NULL)
  Data:   (none)

Field 3 (bool):
  Length: 0x00000001 (1 byte)
  Data:   0x00 (false)

Trailer: 0xFFFF
```

## Source Code References

The authoritative implementations of binary send/recv functions can be found in PostgreSQL source code:

- **Integers**: `src/backend/utils/adt/int.c`, `src/backend/utils/adt/int8.c`
- **Floating Point**: `src/backend/utils/adt/float.c`
- **Boolean**: `src/backend/utils/adt/bool.c`
- **Text**: `src/backend/utils/adt/varlena.c`
- **Format Utilities**: `src/backend/libpq/pqformat.c`

## Version Compatibility

### PostgreSQL Version Support
- Binary format introduced in PostgreSQL 7.4
- Format has remained stable across major versions
- Extensions and new types may be added but don't break existing format
- Always validate signature to detect format changes

### Endianness Considerations
- All multi-byte integers use network byte order (big-endian)
- IEEE 754 floating point format is platform-independent
- Text is always UTF-8 regardless of server encoding
- Boolean encoding may vary but is typically 0x00/0x01

## Testing Considerations

### Test Data Generation
```sql
-- Generate test binary data files
COPY (SELECT 1::int2, 1000::int4, 1000000000::int8) TO '/tmp/int_test.bin' (FORMAT BINARY);
COPY (SELECT 3.14::float4, 3.141592653589793::float8) TO '/tmp/float_test.bin' (FORMAT BINARY);
COPY (SELECT 'Hello'::text, 'World'::text) TO '/tmp/text_test.bin' (FORMAT BINARY);
COPY (SELECT true::bool, false::bool) TO '/tmp/bool_test.bin' (FORMAT BINARY);
COPY (SELECT NULL::int4, NULL::text, NULL::bool) TO '/tmp/null_test.bin' (FORMAT BINARY);
```

### Validation Tools
```bash
# View binary file in hex format
hexdump -C test_file.bin

# Compare with PostgreSQL pg_dump output
pg_dump -t table_name --data-only -f test.sql
```

This reference provides the foundational knowledge needed to implement a robust PostgreSQL binary format parser for the PGArrow project.