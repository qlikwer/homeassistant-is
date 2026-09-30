"""Свежие кадры камер двора из realtime-потока (Flussonic MSE) через ffmpeg.

Снимок ``SNAPSHOT.LIVE.MAIN`` обновляется только по ключевому кадру камеры
(≈ раз в 5 с), поэтому распознавание лица запаздывает. Здесь держится одно
websocket-соединение с fMP4-потоком, видеофрагменты передаются в ffmpeg, а он
отдаёт JPEG ~2 раза в секунду. Держится только последний кадр.

URL потока содержит временный токен и никогда не пишется в лог.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import struct
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from aiohttp import ClientError, ClientSession, WSMsgType

from .const import (
    STREAM_FRAME_FPS,
    STREAM_FRAME_MAX_AGE_SECONDS,
    STREAM_FRAME_STALL_SECONDS,
)

_LOGGER = logging.getLogger("custom_components.intersvyaz.yard_frame_stream")

_RETRY_DELAYS = (2.0, 5.0, 10.0, 30.0)
_REFRESH_AFTER_FAILURES = 3
_JPEG_START = b"\xff\xd8"
_JPEG_END = b"\xff\xd9"


@dataclass(slots=True)
class StreamFrame:
    """Последний декодированный кадр."""

    data: bytes
    seq: int
    received_at: float


def fragment_track_id(fragment: bytes) -> int | None:
    """Вернуть track_id из ``moof/traf/tfhd`` или None, если это не фрагмент."""

    def children(buf: bytes, start: int, end: int):
        pos = start
        while pos + 8 <= end:
            size, kind = struct.unpack(">I4s", buf[pos : pos + 8])
            if size < 8 or pos + size > end:
                return
            yield kind, pos + 8, pos + size
            pos += size

    for kind, start, end in children(fragment, 0, len(fragment)):
        if kind != b"moof":
            continue
        for sub_kind, sub_start, sub_end in children(fragment, start, end):
            if sub_kind != b"traf":
                continue
            for leaf_kind, leaf_start, leaf_end in children(
                fragment, sub_start, sub_end
            ):
                if leaf_kind == b"tfhd" and leaf_end - leaf_start >= 8:
                    return struct.unpack(">I", fragment[leaf_start + 4 : leaf_start + 8])[0]
    return None


class JpegSplitter:
    """Собирает отдельные JPEG из непрерывного потока байт ffmpeg."""

    def __init__(self) -> None:
        self._buffer = b""

    def feed(self, chunk: bytes) -> list[bytes]:
        self._buffer += chunk
        frames: list[bytes] = []
        while True:
            start = self._buffer.find(_JPEG_START)
            if start < 0:
                self._buffer = b""
                break
            end = self._buffer.find(_JPEG_END, start + 2)
            if end < 0:
                self._buffer = self._buffer[start:]
                break
            frames.append(self._buffer[start : end + 2])
            self._buffer = self._buffer[end + 2 :]
        return frames


class YardFrameStream:
    """Одно постоянное соединение с камерой и ffmpeg-декодером."""

    def __init__(
        self,
        session: ClientSession,
        *,
        camera_ref: str,
        get_url: Callable[[], str | None],
        ffmpeg_binary: str,
        on_repeated_failure: Callable[[], Awaitable[object]] | None = None,
    ) -> None:
        self._session = session
        self._camera_ref = camera_ref
        self._get_url = get_url
        self._ffmpeg = ffmpeg_binary
        self._on_repeated_failure = on_repeated_failure
        self._task: asyncio.Task | None = None
        self._latest: StreamFrame | None = None
        self._seq = 0

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def latest(self, max_age: float = STREAM_FRAME_MAX_AGE_SECONDS) -> StreamFrame | None:
        frame = self._latest
        if frame is None or time.monotonic() - frame.received_at > max_age:
            return None
        return frame

    def start(self, hass_create_task: Callable[..., asyncio.Task]) -> None:
        if not self.running:
            self._task = hass_create_task(
                self._run(), f"intersvyaz_frame_stream_{self._camera_ref}"
            )

    async def async_stop(self) -> None:
        task, self._task = self._task, None
        self._latest = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _run(self) -> None:
        failures = 0
        while True:
            started = time.monotonic()
            try:
                await self._session_once()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — поток не должен ронять HA
                _LOGGER.debug(
                    "[FRAME_STREAM][ERROR] camera=%s %s",
                    self._camera_ref,
                    type(err).__name__,
                )
            # Сессия, проработавшая заметное время, сбрасывает счётчик ошибок.
            failures = 0 if time.monotonic() - started > 60 else failures + 1
            if failures == _REFRESH_AFTER_FAILURES and self._on_repeated_failure:
                # Скорее всего протух токен в URL: обновляем каталог камер.
                with contextlib.suppress(Exception):
                    await self._on_repeated_failure()
            delay = _RETRY_DELAYS[min(max(failures - 1, 0), len(_RETRY_DELAYS) - 1)]
            await asyncio.sleep(delay)

    async def _session_once(self) -> None:
        url = self._get_url()
        if not url:
            raise ClientError("no stream url")

        proc = await asyncio.create_subprocess_exec(
            self._ffmpeg,
            "-loglevel", "error",
            "-fflags", "nobuffer",
            "-flags", "low_delay",
            "-probesize", "32768",
            "-analyzeduration", "0",
            "-f", "mp4",
            "-i", "pipe:0",
            "-vf", f"fps={STREAM_FRAME_FPS}",
            "-q:v", "4",
            "-f", "image2pipe",
            "-vcodec", "mjpeg",
            "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        reader = asyncio.create_task(self._read_frames(proc))
        try:
            async with self._session.ws_connect(
                url, timeout=10, heartbeat=None
            ) as ws:
                init = await ws.receive(timeout=10)
                if init.type != WSMsgType.TEXT:
                    raise ClientError("no init segment")
                tracks = json.loads(init.data).get("tracks") or []
                video = next((t for t in tracks if t.get("content") == "video"), None)
                if not video or not video.get("payload"):
                    raise ClientError("no video track")
                video_id = video.get("id")
                proc.stdin.write(base64.b64decode(video["payload"]))
                await ws.send_str("resume")
                _LOGGER.info(
                    "[FRAME_STREAM][CONNECTED] camera=%s", self._camera_ref
                )
                while True:
                    msg = await ws.receive(timeout=STREAM_FRAME_STALL_SECONDS)
                    if msg.type == WSMsgType.BINARY:
                        if fragment_track_id(msg.data) == video_id:
                            proc.stdin.write(msg.data)
                            await proc.stdin.drain()
                    elif msg.type != WSMsgType.TEXT:
                        raise ClientError("stream closed")
        finally:
            reader.cancel()
            with contextlib.suppress(Exception):
                proc.stdin.close()
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await proc.wait()

    async def _read_frames(self, proc: asyncio.subprocess.Process) -> None:
        splitter = JpegSplitter()
        while True:
            chunk = await proc.stdout.read(65536)
            if not chunk:
                return
            for data in splitter.feed(chunk):
                self._seq += 1
                self._latest = StreamFrame(data, self._seq, time.monotonic())


class YardFrameStreamManager:
    """Запускает потоки по требованию и отдаёт свежие кадры."""

    def __init__(
        self,
        session: ClientSession,
        create_task: Callable[..., asyncio.Task],
        ffmpeg_binary: Callable[[], str | None],
    ) -> None:
        self._session = session
        self._create_task = create_task
        self._ffmpeg_binary = ffmpeg_binary
        self._streams: dict[str, YardFrameStream] = {}

    def frame(
        self,
        camera_uid: str,
        *,
        get_url: Callable[[], str | None],
        on_repeated_failure: Callable[[], Awaitable[object]] | None = None,
    ) -> StreamFrame | None:
        """Свежий кадр или None (поток при этом запускается, если ещё не идёт)."""

        stream = self._streams.get(camera_uid)
        if stream is None:
            binary = self._ffmpeg_binary()
            if not binary:
                return None
            ref = hashlib.sha256(camera_uid.encode("utf-8")).hexdigest()[:12]
            stream = YardFrameStream(
                self._session,
                camera_ref=ref,
                get_url=get_url,
                ffmpeg_binary=binary,
                on_repeated_failure=on_repeated_failure,
            )
            self._streams[camera_uid] = stream
        stream.start(self._create_task)
        return stream.latest()

    async def async_retain(self, camera_uids: set[str]) -> None:
        """Остановить потоки камер, которые больше не анализируются."""

        for uid in [uid for uid in self._streams if uid not in camera_uids]:
            await self._streams.pop(uid).async_stop()

    async def async_stop(self) -> None:
        await self.async_retain(set())
