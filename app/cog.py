import struct

# Mapping from TIFF types to `struct`
TAG_TYPES = {
    1: {"format": "B", "length": 1},
    2: {"format": "c", "length": 1},
    3: {"format": "H", "length": 2},
    4: {"format": "L", "length": 4},
    5: {"format": "f", "length": 4},
    7: {"format": "B", "length": 1},
    12: {"format": "d", "length": 8},
    16: {"format": "Q", "length": 8},
}


def is_bigtiff(b: bytes) -> bool:
    endian = "big" if b[:2] == b"MM" else "little"
    tiff_version = int.from_bytes(b[0:2], endian)
    return tiff_version == 43


def extract_byte_ranges(b: bytes) -> list[tuple[int, int]]:
    """Parse the TIFF header and extracts byte ranges that should be saved to redis.
    This includes TileOffsets (#324) and TileByteCounts (#325)
    """
    byte_ranges_to_save = []

    endian = "big" if b[:2] == b"MM" else "little"
    _endian = ">" if endian == "big" else "<"
    next_ifd_offset = int.from_bytes(b[4:8], endian)
    while next_ifd_offset != 0:
        # First 2 bytes contain number of tags in the IFD.
        tag_count = int.from_bytes(b[next_ifd_offset : next_ifd_offset + 1], endian)

        tags = {}
        for idx in range(tag_count):
            # Tags are always 12 bytes each.
            tag_start = next_ifd_offset + 2 + (12 * idx)

            # First 2 bytes contain tag code.
            tag_code = int.from_bytes(b[tag_start : tag_start + 2], endian)

            # (TileOffsets, TileByteCounts)
            if tag_code in (324, 325):
                # Bytes 2-4 contain the tag's data type.
                data_type = int.from_bytes(b[tag_start + 2 : tag_start + 4], endian)

                # Bytes 4-8 contain the number of values in the tag.
                count = int.from_bytes(b[tag_start + 4 : tag_start + 8], endian)
                size = count * TAG_TYPES[data_type]["length"]

                # Bytes 8-12 contain the tag value if it fits, otherwise it
                # contains an offset to where the tag value is stored
                if size <= 4:
                    tag_value = b[tag_start + 8 : tag_start + 8 + size]
                else:
                    value_offset = int.from_bytes(
                        b[tag_start + 8 : tag_start + 12], endian
                    )
                    tag_value = b[value_offset : value_offset + size]

                # Decode the tag value
                tags[tag_code] = struct.unpack(
                    f"{_endian}{count}{TAG_TYPES[data_type]['format']}", tag_value
                )
        byte_ranges_to_save.extend(list(zip(tags[324], tags[325])))

        # Last 4 bytes of IFD contains offset to the next IFD
        next_ifd_offset = int.from_bytes(b[tag_start + 12 : tag_start + 12 + 4], endian)

    return byte_ranges_to_save
