"""FCM push Интерсвязи: событие входящего звонка.

Интеграция регистрируется у провайдера как ещё одно устройство аккаунта
(`PUT td-crm.is74.ru/api/user-device`) и слушает Firebase Cloud Messaging.
Идентификаторы Firebase-проекта приложения Интерсвязи вводит пользователь в
настройках; без них менеджер ничего не делает. Токены и ключи в лог не пишутся.
"""
from __future__ import annotations

from datetime import timedelta
import logging
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_call_later, async_track_time_interval
from homeassistant.helpers.storage import Store

from .api import IntersvyazApiClient
from .api_errors import IntersvyazApiError
from .const import (
    CONF_FCM_API_KEY,
    CONF_FCM_APP_ID,
    CONF_FCM_PROJECT_ID,
    CONF_FCM_SENDER_ID,
    DOMAIN,
    DOOR_EVENT_INCOMING_CALL,
    EVENT_PUSH_RECEIVED,
    FCM_MAINTENANCE_INTERVAL_HOURS,
    FCM_RETRY_DELAY_MINUTES,
)
from .events import emit_door_event
from .push_parser import parse_push, resolve_door_uid
from .runtime import IntersvyazConfigEntry
from .security import redact_mapping

_LOGGER = logging.getLogger(f"custom_components.{DOMAIN}.push")
_STORE_VERSION = 1


class IntersvyazPushManager:
    """Слушатель FCM одного аккаунта."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: IntersvyazConfigEntry,
        api: IntersvyazApiClient,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._api = api
        self._store: Store[dict[str, Any]] = Store(
            hass, _STORE_VERSION, f"{DOMAIN}_fcm_{entry.entry_id}"
        )
        self._client: Any = None
        self._unsub_maintenance = None
        self._unsub_retry = None
        self._stopped = True
        self._last_app_data: dict[str, str] = {}

    @property
    def configured(self) -> bool:
        options = self._entry.options
        return all(
            str(options.get(key) or "").strip()
            for key in (
                CONF_FCM_PROJECT_ID,
                CONF_FCM_APP_ID,
                CONF_FCM_API_KEY,
                CONF_FCM_SENDER_ID,
            )
        )

    @property
    def running(self) -> bool:
        return self._client is not None and not self._stopped

    async def async_start(self) -> None:
        """Запустить слушатель, если заданы идентификаторы Firebase."""

        self._stopped = False
        if not self.configured:
            _LOGGER.debug("FCM не настроен: слушатель не запускается")
            return
        try:
            from firebase_messaging import FcmPushClient, FcmRegisterConfig
        except ImportError:
            _LOGGER.warning(
                "Пакет firebase-messaging не установлен: push о звонках недоступны"
            )
            return

        options = self._entry.options
        credentials = await self._store.async_load()
        try:
            config = FcmRegisterConfig(
                str(options[CONF_FCM_PROJECT_ID]).strip(),
                str(options[CONF_FCM_APP_ID]).strip(),
                str(options[CONF_FCM_API_KEY]).strip(),
                str(options[CONF_FCM_SENDER_ID]).strip(),
            )
            client = self._build_client(
                FcmPushClient, config, credentials
            )
            fcm_token = await client.checkin_or_register()
            await self._api.async_register_push_device(fcm_token)
            await client.start()
        except IntersvyazApiError as err:
            _LOGGER.warning(
                "Не удалось зарегистрировать push-устройство: %s; повтор через %s мин",
                type(err).__name__,
                FCM_RETRY_DELAY_MINUTES,
            )
            self._schedule_retry()
            return
        except Exception as err:  # noqa: BLE001 — сбой FCM не должен ронять домофон
            _LOGGER.warning(
                "FCM не запущен: %s; повтор через %s мин",
                type(err).__name__,
                FCM_RETRY_DELAY_MINUTES,
            )
            self._schedule_retry()
            return

        self._client = client
        self._unsub_maintenance = async_track_time_interval(
            self._hass,
            self._async_maintenance,
            timedelta(hours=FCM_MAINTENANCE_INTERVAL_HOURS),
        )
        _LOGGER.info("FCM-слушатель звонков запущен")

    def _build_client(self, client_cls: Any, config: Any, credentials: Any) -> Any:
        """Клиент, дополнительно сохраняющий app_data сообщения.

        Библиотека отдаёт в callback только расшифрованное тело, а служебные
        и пользовательские поля push лежат в app_data (ключ-значение). Звонок
        может быть закодирован именно там, поэтому забираем их до расшифровки.
        """

        manager = self

        class _Client(client_cls):  # type: ignore[misc, valid-type]
            def _handle_data_message(self, msg: Any) -> None:
                try:
                    manager._last_app_data = {
                        str(item.key): str(item.value) for item in msg.app_data
                    }
                except Exception:  # noqa: BLE001
                    manager._last_app_data = {}
                try:
                    super()._handle_data_message(msg)
                except RuntimeError:
                    # Нет ключей шифрования: тело недоступно, но app_data есть.
                    manager._on_notification({})

        return _Client(
            self._on_notification,
            config,
            credentials,
            self._on_credentials_updated,
        )

    async def async_stop(self) -> None:
        self._stopped = True
        for unsub in (self._unsub_maintenance, self._unsub_retry):
            if unsub is not None:
                unsub()
        self._unsub_maintenance = None
        self._unsub_retry = None
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.stop()
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Остановка FCM: %s", type(err).__name__)

    async def async_restart(self) -> None:
        """Применить изменённые в настройках идентификаторы."""

        await self.async_stop()
        await self.async_start()

    def _schedule_retry(self) -> None:
        if self._stopped:
            return
        if self._unsub_retry is not None:
            self._unsub_retry()

        async def _retry(_now: Any) -> None:
            self._unsub_retry = None
            if not self._stopped:
                await self.async_restart()

        self._unsub_retry = async_call_later(
            self._hass, timedelta(minutes=FCM_RETRY_DELAY_MINUTES), _retry
        )

    async def _async_maintenance(self, _now: Any) -> None:
        """Раз в 12 часов заново сообщить провайдеру токен и проверить слушатель."""

        if self._stopped or self._client is None:
            return
        try:
            token = await self._client.checkin_or_register()
            await self._api.async_register_push_device(token)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Обслуживание FCM не удалось: %s", type(err).__name__)
            await self.async_restart()

    def _on_credentials_updated(self, credentials: dict[str, Any]) -> None:
        self._hass.loop.call_soon_threadsafe(
            lambda: self._store.async_delay_save(lambda: credentials, 1)
        )

    def _on_notification(self, *args: Any) -> None:
        # Библиотека может вызывать callback не из цикла Home Assistant.
        self._hass.loop.call_soon_threadsafe(self._handle_push, args)

    @callback
    def _handle_push(self, args: tuple[Any, ...]) -> None:
        app_data = self._last_app_data
        self._last_app_data = {}
        try:
            payload = parse_push(*args, {"app_data": app_data})
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Не удалось разобрать push: %s", type(err).__name__)
            return

        _LOGGER.info(
            "Получен push: поля=%s is_call=%s", payload["fields"], payload["is_call"]
        )
        # Диагностическое событие: содержимое видно только в Home Assistant
        # («Инструменты разработчика → События → Слушать intersvyaz_push_received»).
        self._hass.bus.async_fire(
            EVENT_PUSH_RECEIVED,
            {
                "entry_id": self._entry.entry_id,
                "fields": payload["fields"],
                "title": payload["title"],
                "body": payload["body"],
                "push_type": payload["push_type"],
                "is_call": payload["is_call"],
                "app_data_keys": sorted(app_data),
                "data": redact_mapping(payload["data"]),
            },
        )
        if not payload["is_call"]:
            return

        runtime = self._entry.runtime_data
        door_uid = resolve_door_uid(runtime.doors, payload)
        if door_uid is None:
            _LOGGER.warning("Звонок получен, но домофон не определён")
            return
        emit_door_event(
            self._hass,
            self._entry,
            door_uid,
            DOOR_EVENT_INCOMING_CALL,
            {
                "title": payload["title"],
                "body": payload["body"],
                "entrance": payload["entrance"],
            },
        )
