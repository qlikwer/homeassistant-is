"""Фоновая обработка снимков выбранных камер Intersvyaz."""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Callable

from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    CAMERA_FRAME_INTERVAL_SECONDS,
    CONF_BACKGROUND_CAMERAS,
    CONF_FRAME_SOURCE,
    DEFAULT_FRAME_SOURCE,
    FRAME_SOURCE_STREAM,
    RECOGNITION_MODE_OFF,
    STREAM_FRAME_INTERVAL_SECONDS,
)
from .runtime import IntersvyazConfigEntry
from .security import safe_door_ref

_LOGGER = logging.getLogger("custom_components.intersvyaz.background")


class DoorBackgroundProcessor:
    """Периодически получает кадры и передаёт их локальному recognition engine."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: IntersvyazConfigEntry,
        *,
        interval_seconds: float = CAMERA_FRAME_INTERVAL_SECONDS,
        scheduler: Callable = async_track_time_interval,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._interval = max(float(interval_seconds), 1.0)
        self._scheduler = scheduler
        self._unsubscribe: Callable[[], None] | None = None
        self._selected_uids: set[str] = set()
        self._lock = asyncio.Lock()
        self._pending = False
        self._snapshot_failures: dict[str, int] = {}
        self._subscribed_interval: float | None = None
        self._last_stream_seq: dict[str, int] = {}

    @property
    def selected_uids(self) -> set[str]:
        return set(self._selected_uids)

    async def async_setup(self) -> None:
        await self.async_refresh_from_options(initial=True)

    def async_stop(self) -> None:
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None
        self._selected_uids.clear()
        self._pending = False
        self._last_stream_seq.clear()
        self._stop_streams()

    def _stream_mode(self) -> bool:
        source = self._entry.options.get(CONF_FRAME_SOURCE, DEFAULT_FRAME_SOURCE)
        return source == FRAME_SOURCE_STREAM

    def _effective_interval(self) -> float:
        if self._stream_mode():
            return max(float(STREAM_FRAME_INTERVAL_SECONDS), 1.0)
        return self._interval

    def _stop_streams(self) -> None:
        manager = getattr(self._entry.runtime_data, "frame_stream_manager", None)
        if manager is not None:
            self._hass.async_create_task(manager.async_stop())

    async def async_refresh_from_options(self, *, initial: bool = False) -> None:
        runtime = self._entry.runtime_data
        manager = runtime.face_manager
        if manager.recognition_mode == RECOGNITION_MODE_OFF:
            self.async_stop()
            _LOGGER.info("Фоновое распознавание выключено mode=off")
            return

        yard_available = {
            camera.uid: camera
            for camera in runtime.live_yard_cameras
            if bool(camera.snapshot_url)
        }
        # Двор и обычные домофоны — разные API-источники одного аккаунта, а не
        # взаимоисключающие варианты: домофон без сопоставленной камеры двора
        # (например, "шаренный"/дополнительный домофон с другого адреса) должен
        # оставаться доступным для опроса, даже если для других домофонов
        # аккаунта камеры двора есть. См. тот же union в options_flow.py.
        matched_door_uids = {
            camera.matched_door_uid
            for camera in yard_available.values()
            if camera.matched_door_uid
        }
        door_available = {
            door.uid: door
            for door in runtime.doors
            if door.has_video
            and bool(door.image_url)
            and door.uid not in matched_door_uids
        }
        available_uids = set(yard_available) | set(door_available)
        if not available_uids:
            self.async_stop()
            return

        option_value = self._entry.options.get(CONF_BACKGROUND_CAMERAS)
        if isinstance(option_value, list):
            desired: set[str] = set()
            for uid in option_value:
                if uid in available_uids:
                    desired.add(uid)
                    continue
                # 2.0.7 хранил door UID. После появления yard API бесшовно
                # переносим выбор на камеру, сопоставленную с тем же домофоном.
                if yard_available:
                    mapped = next(
                        (
                            camera.uid
                            for camera in yard_available.values()
                            if camera.matched_door_uid == uid
                        ),
                        None,
                    )
                    if mapped:
                        desired.add(mapped)
        elif manager.list_known_face_names():
            if yard_available:
                main_door = runtime.default_door
                main_camera = next(
                    (
                        camera
                        for camera in yard_available.values()
                        if main_door and camera.matched_door_uid == main_door.uid
                    ),
                    None,
                )
                desired = {main_camera.uid if main_camera else next(iter(yard_available))}
            else:
                main = next((door for door in door_available.values() if door.is_main), None)
                desired = {main.uid if main else next(iter(door_available))}
            if not initial:
                _LOGGER.info("Применена fallback background camera для старой конфигурации")
        else:
            desired = set()

        self._apply_selection(desired)
        await self._async_retain_streams()

    async def _async_retain_streams(self) -> None:
        manager = getattr(self._entry.runtime_data, "frame_stream_manager", None)
        if manager is None:
            return
        keep = self._selected_uids if self._stream_mode() else set()
        await manager.async_retain(set(keep))

    def _apply_selection(self, desired: set[str]) -> None:
        interval = self._effective_interval()
        if (
            desired == self._selected_uids
            and self._unsubscribe
            and interval == self._subscribed_interval
        ):
            return
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None
        self._selected_uids = set(desired)
        if not self._selected_uids:
            _LOGGER.info("Фоновая обработка камер отключена")
            return
        self._unsubscribe = self._scheduler(
            self._hass,
            self._async_schedule_handler,
            timedelta(seconds=interval),
        )
        self._subscribed_interval = interval
        _LOGGER.info(
            "Фоновая обработка включена: entry_id=%s cameras=%s interval=%.1fs",
            self._entry.entry_id,
            len(self._selected_uids),
            interval,
        )

    async def _async_schedule_handler(self, _now: Any) -> None:
        await self._async_process_selected()

    async def async_force_cycle(self) -> None:
        await self._async_process_selected()

    async def _async_process_selected(self) -> None:
        if not self._selected_uids:
            return
        if self._lock.locked():
            self._pending = True
            return

        run_again = False
        async with self._lock:
            try:
                runtime = self._entry.runtime_data
                for uid in list(self._selected_uids):
                    target = self._resolve_target(uid)
                    if target is None:
                        self._selected_uids.discard(uid)
                        continue
                    snapshot_uid, recognition_uid, image_url, callback, refresh = target
                    image = None
                    if self._stream_mode():
                        frame = self._stream_frame(uid)
                        if frame is not None:
                            if frame.seq == self._last_stream_seq.get(uid):
                                continue  # этот кадр уже обработан
                            self._last_stream_seq[uid] = frame.seq
                            image = frame.data
                    if image is None:
                        image = await runtime.snapshot_manager.async_get_snapshot(
                            snapshot_uid,
                            image_url,
                        )
                    if not image:
                        failures = self._snapshot_failures.get(uid, 0) + 1
                        self._snapshot_failures[uid] = failures
                        if failures >= 3:
                            _LOGGER.info(
                                "Три ошибки снимка source=%s; обновляем временные ссылки",
                                safe_door_ref(uid),
                            )
                            self._snapshot_failures[uid] = 0
                            await refresh()
                        continue
                    self._snapshot_failures[uid] = 0
                    await runtime.face_manager.async_process_image(
                        recognition_uid,
                        image,
                        callback,
                    )
            finally:
                run_again = self._pending
                self._pending = False

        if run_again:
            self._hass.async_create_task(self._async_process_selected())

    def _stream_frame(self, uid: str):
        """Свежий кадр из realtime-потока; None → используем обычный снимок."""

        runtime = self._entry.runtime_data
        manager = getattr(runtime, "frame_stream_manager", None)
        camera = runtime.yard_camera_manager.get(uid)
        if manager is None or camera is None or not camera.mse_url:
            return None

        def get_url() -> str | None:
            current = runtime.yard_camera_manager.get(uid)
            return current.mse_url if current is not None else None

        return manager.frame(
            uid,
            get_url=get_url,
            on_repeated_failure=runtime.yard_camera_manager.async_refresh,
        )

    def _resolve_target(self, uid: str):
        runtime = self._entry.runtime_data
        camera = runtime.yard_camera_manager.get(uid)
        if camera is not None and camera.snapshot_url:
            if camera.matched_door_uid:
                door = runtime.door_manager.get(camera.matched_door_uid)
                if door is not None:
                    return (
                        camera.uid,
                        door.uid,
                        camera.snapshot_url,
                        door.callback,
                        runtime.yard_camera_manager.async_refresh,
                    )
            return (
                camera.uid,
                camera.uid,
                camera.snapshot_url,
                None,
                runtime.yard_camera_manager.async_refresh,
            )

        door = runtime.door_manager.get(uid)
        if door is not None and door.image_url:
            return (
                door.uid,
                door.uid,
                door.image_url,
                door.callback,
                runtime.door_manager.async_refresh,
            )
        return None
