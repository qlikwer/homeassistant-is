"""Разбор fMP4-фрагментов и JPEG-потока для кадров из realtime-стрима."""

import struct

from custom_components.intersvyaz.yard_frame_stream import (
    JpegSplitter,
    fragment_track_id,
)


def _box(kind: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", 8 + len(payload), kind) + payload


def _fragment(track_id: int) -> bytes:
    tfhd = _box(b"tfhd", struct.pack(">II", 0, track_id))
    traf = _box(b"traf", tfhd)
    mfhd = _box(b"mfhd", struct.pack(">II", 0, 1))
    return _box(b"moof", mfhd + traf) + _box(b"mdat", b"\x00" * 16)


def test_fragment_track_id_reads_tfhd() -> None:
    assert fragment_track_id(_fragment(1)) == 1
    assert fragment_track_id(_fragment(2)) == 2


def test_fragment_track_id_ignores_non_fragments() -> None:
    assert fragment_track_id(b"") is None
    assert fragment_track_id(b"\x00\x00\x00\x08free") is None
    assert fragment_track_id(_box(b"mdat", b"abcdef")) is None
    # Обрезанный/битый заголовок не должен падать.
    assert fragment_track_id(_fragment(1)[:12]) is None


def test_jpeg_splitter_joins_and_splits_frames() -> None:
    frame_a = b"\xff\xd8" + b"A" * 10 + b"\xff\xd9"
    frame_b = b"\xff\xd8" + b"B" * 5 + b"\xff\xd9"
    splitter = JpegSplitter()
    stream = frame_a + frame_b
    assert splitter.feed(stream[:7]) == []
    assert splitter.feed(stream[7:20]) == [frame_a]
    assert splitter.feed(stream[20:]) == [frame_b]


def test_jpeg_splitter_drops_garbage_between_frames() -> None:
    frame = b"\xff\xd8" + b"X" * 4 + b"\xff\xd9"
    splitter = JpegSplitter()
    assert splitter.feed(b"junk" + frame + b"tail") == [frame]
