"""The tractive integration."""

import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging
from typing import TYPE_CHECKING, Any

import aiotractive

from homeassistant.components.sensor import DOMAIN as SENSOR_DOMAIN
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    ATTR_BATTERY_CHARGING,
    ATTR_BATTERY_LEVEL,
    CONF_EMAIL,
    CONF_PASSWORD,
    EVENT_HOMEASSISTANT_STOP,
    Platform,
)
from homeassistant.core import CALLBACK_TYPE, Event, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr, entity_registry as er
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_call_later

from .const import (
    ATTR_DAILY_GOAL,
    ATTR_MINUTES_ACTIVE,
    ATTR_MINUTES_DAY_SLEEP,
    ATTR_MINUTES_NIGHT_SLEEP,
    ATTR_MINUTES_REST,
    ATTR_POWER_SAVING,
    ATTR_TRACKER_STATE,
    CLIENT_ID,
    DOMAIN,
    RECONNECT_INTERVAL,
    SERVER_UNAVAILABLE,
    SWITCH_KEY_MAP,
    TRACKER_HARDWARE_STATUS_UPDATED,
    TRACKER_HEALTH_OVERVIEW_UPDATED,
    TRACKER_POSITION_UPDATED,
    TRACKER_SWITCH_STATUS_UPDATED,
    UNAVAILABLE_AFTER,
)

PLATFORMS = [
    Platform.BINARY_SENSOR,
    Platform.DEVICE_TRACKER,
    Platform.SENSOR,
    Platform.SWITCH,
]


_LOGGER = logging.getLogger(__name__)


@dataclass
class Trackables:
    """A class that describes trackables."""

    tracker: aiotractive.tracker.Tracker
    trackable: dict[str, Any]
    tracker_details: dict[str, Any]
    hw_info: dict[str, Any]
    pos_report: dict[str, Any]
    health_overview: dict[str, Any]


@dataclass(slots=True)
class TractiveData:
    """Class for Tractive data."""

    client: TractiveClient
    trackables: list[Trackables]


type TractiveConfigEntry = ConfigEntry[TractiveData]


async def async_setup_entry(hass: HomeAssistant, entry: TractiveConfigEntry) -> bool:
    """Set up tractive from a config entry."""
    data = entry.data

    client = aiotractive.Tractive(
        data[CONF_EMAIL],
        data[CONF_PASSWORD],
        session=async_get_clientsession(hass),
        client_id=CLIENT_ID,
    )
    try:
        creds = await client.authenticate()
    except aiotractive.exceptions.UnauthorizedError as error:
        await client.close()
        raise ConfigEntryAuthFailed from error
    except aiotractive.exceptions.TractiveError as error:
        await client.close()
        raise ConfigEntryNotReady from error

    if TYPE_CHECKING:
        assert creds is not None

    tractive = TractiveClient(hass, client, creds["user_id"], entry)

    trackables = []
    try:
        for obj in await client.trackable_objects():
            # To avoid hitting Tractive API rate limits, we add a small
            # delay between requests to fetch trackable details.
            await asyncio.sleep(2)
            trackables.append(await _generate_trackables(client, obj))
    except aiotractive.exceptions.TractiveError as error:
        await client.close()
        raise ConfigEntryNotReady from error
    except ConfigEntryNotReady:
        await client.close()
        raise

    # When the pet defined in Tractive has no tracker linked we get None as `trackable`.
    # So we have to remove None values from trackables list.
    filtered_trackables = [item for item in trackables if item]

    entry.runtime_data = TractiveData(tractive, filtered_trackables)

    # Register the tracker devices so entities on the pet devices can resolve
    # their via_device link at construction time.
    device_registry = dr.async_get(hass)
    for item in filtered_trackables:
        device_registry.async_get_or_create(
            config_entry_id=entry.entry_id,
            configuration_url="https://my.tractive.com/",
            identifiers={(DOMAIN, item.tracker_details["_id"])},
            translation_key="tracker",
            translation_placeholders={"id": item.tracker_details["_id"]},
            manufacturer="Tractive GmbH",
            sw_version=item.tracker_details["fw_version"],
            model_id=item.tracker_details["model_number"],
        )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Send initial health overview data to sensors after platforms are set up
    for item in filtered_trackables:
        if item.health_overview:
            tractive.send_health_overview_update(item.health_overview)

    async def cancel_listen_task(_: Event) -> None:
        await tractive.unsubscribe()

    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, cancel_listen_task)
    )
    entry.async_on_unload(tractive.unsubscribe)

    # Remove sensor entities that are no longer supported by the Tractive API
    entity_reg = er.async_get(hass)
    for item in filtered_trackables:
        for key in ("activity_label", "calories", "sleep_label"):
            if entity_id := entity_reg.async_get_entity_id(
                SENSOR_DOMAIN, DOMAIN, f"{item.trackable['_id']}_{key}"
            ):
                entity_reg.async_remove(entity_id)

    return True


async def _generate_trackables(
    client: aiotractive.Tractive,
    trackable: aiotractive.trackable_object.TrackableObject,
) -> Trackables | None:
    """Generate trackables."""
    trackable_data = await trackable.details()

    # Check that the pet has tracker linked.
    if not trackable_data.get("device_id"):
        return None

    if "details" not in trackable_data:
        _LOGGER.warning(
            "Tracker %s has no details and will be"
            " skipped. This happens for shared trackers",
            trackable_data["device_id"],
        )
        return None

    tracker = client.tracker(trackable_data["device_id"])
    trackable_pet = client.trackable_object(trackable_data["_id"])

    # Sequential fetching to prevent HTTP 429 Rate Limits
    tracker_details = await tracker.details()
    hw_info = await tracker.hw_info()
    pos_report = await tracker.pos_report()
    health_overview = await trackable_pet.health_overview()

    if not tracker_details.get("_id"):
        raise ConfigEntryNotReady(
            "Tractive API returns incomplete data"
            f" for tracker {trackable_data['device_id']}",
        )

    return Trackables(
        tracker, trackable_data, tracker_details, hw_info, pos_report, health_overview
    )


async def async_unload_entry(hass: HomeAssistant, entry: TractiveConfigEntry) -> bool:
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


class TractiveClient:
    """A Tractive client."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: aiotractive.Tractive,
        user_id: str,
        config_entry: TractiveConfigEntry,
    ) -> None:
        """Initialize the client."""
        self._hass = hass
        self._client = client
        self._user_id = user_id
        self._last_hw_time = 0
        self._last_pos_time = 0
        self._listen_task: asyncio.Task | None = None
        self._config_entry = config_entry
        self._cancel_unavailable_timer: CALLBACK_TYPE | None = None
        self._unavailable_sent = False

    @property
    def user_id(self) -> str:
        """Return user id."""
        return self._user_id

    @property
    def subscribed(self) -> bool:
        """Return True if subscribed."""
        if self._listen_task is None:
            return False

        return not self._listen_task.cancelled()

    def subscribe(self) -> None:
        """Start event listener coroutine."""
        self._listen_task = asyncio.create_task(self._listen())

    async def unsubscribe(self) -> None:
        """Stop event listener coroutine."""
        if self._listen_task:
            self._listen_task.cancel()
        self._stop_unavailable_timer()
        await self._client.close()

    async def _listen(self) -> None:
        connection_lost = False
        while True:
            try:
                async for event in self._client.events():
                    _LOGGER.debug("Received event: %s", event)
                    # The channel opens with the full status of every tracker,
                    # so the first event shows the connection is back
                    if connection_lost:
                        connection_lost = False
                        self._stop_unavailable_timer()
                        if self._unavailable_sent:
                            _LOGGER.info("Tractive is back online")
                            self._unavailable_sent = False
                            await self._async_refresh_health_overview()
                    if event["message"] == "health_overview":
                        self.send_health_overview_update(event)
                        continue
                    if (
                        "hardware" in event
                        and self._last_hw_time != event["hardware"]["time"]
                    ):
                        self._last_hw_time = event["hardware"]["time"]
                        self._send_hardware_update(event)
                        self._send_switch_update(event)
                    if (
                        "position" in event
                        and self._last_pos_time != event["position"]["time"]
                    ):
                        self._last_pos_time = event["position"]["time"]
                        self._send_position_update(event)
                    # If any key belonging to the switch is present in the event,
                    # we send a switch status update
                    if bool(set(SWITCH_KEY_MAP.values()).intersection(event)):
                        self._send_switch_update(event)
            except aiotractive.exceptions.UnauthorizedError:
                self._config_entry.async_start_reauth(self._hass)
                await self.unsubscribe()
                _LOGGER.error(
                    "Authentication failed for %s, try reconfiguring device",
                    self._config_entry.data[CONF_EMAIL],
                )
                return
            except (KeyError, TypeError) as error:
                _LOGGER.error("Error while listening for events: %s", error)
                continue
            except aiotractive.exceptions.TractiveError:
                if not connection_lost:
                    connection_lost = True
                    _LOGGER.debug(
                        "Tractive is not available. Retrying every %i seconds",
                        RECONNECT_INTERVAL.total_seconds(),
                    )
                    # A timer, because the library may retry connect timeouts
                    # forever without raising again
                    self._cancel_unavailable_timer = async_call_later(
                        self._hass, UNAVAILABLE_AFTER, self._async_mark_unavailable
                    )
                self._last_hw_time = 0
                self._last_pos_time = 0
                await asyncio.sleep(RECONNECT_INTERVAL.total_seconds())
                continue

    @callback
    def _async_mark_unavailable(self, _: datetime) -> None:
        """Mark the entities unavailable after a long outage."""
        self._cancel_unavailable_timer = None
        _LOGGER.info(
            "Tractive has been unavailable for %s, marking entities unavailable",
            UNAVAILABLE_AFTER,
        )
        self._unavailable_sent = True
        async_dispatcher_send(self._hass, f"{SERVER_UNAVAILABLE}-{self._user_id}")

    def _stop_unavailable_timer(self) -> None:
        if self._cancel_unavailable_timer is not None:
            self._cancel_unavailable_timer()
            self._cancel_unavailable_timer = None

    async def _async_refresh_health_overview(self) -> None:
        """Fetch the health overview, which the channel does not replay."""
        for item in self._config_entry.runtime_data.trackables:
            try:
                health_overview = await self._client.trackable_object(
                    item.trackable["_id"]
                ).health_overview()
            except aiotractive.exceptions.UnauthorizedError:
                raise
            except aiotractive.exceptions.TractiveError as error:
                # Not worth dropping a working channel, the next event catches up
                _LOGGER.debug("Failed to refresh the health overview: %s", error)
                continue
            if health_overview:
                self.send_health_overview_update(health_overview)

    def _send_hardware_update(self, event: dict[str, Any]) -> None:
        # Sometimes hardware event doesn't contain complete data.
        payload = {
            ATTR_BATTERY_LEVEL: event["hardware"]["battery_level"],
            ATTR_TRACKER_STATE: event["tracker_state"].lower(),
            ATTR_POWER_SAVING: event.get("tracker_state_reason") == "POWER_SAVING",
            ATTR_BATTERY_CHARGING: event["charging_state"] == "CHARGING",
        }
        self._dispatch_tracker_event(
            TRACKER_HARDWARE_STATUS_UPDATED, event["tracker_id"], payload
        )

    def _send_switch_update(self, event: dict[str, Any]) -> None:
        # Sometimes the event contains data for all switches, sometimes only for one.
        payload = {}
        for switch, key in SWITCH_KEY_MAP.items():
            if switch_data := event.get(key):
                payload[switch] = switch_data["active"]
        if hardware := event.get("hardware", {}):
            payload[ATTR_POWER_SAVING] = (
                hardware.get("power_saving_zone_id") is not None
            )
        self._dispatch_tracker_event(
            TRACKER_SWITCH_STATUS_UPDATED, event["tracker_id"], payload
        )

    def send_health_overview_update(self, event: dict[str, Any]) -> None:
        """Handle health_overview events from Tractive API."""
        # The health_overview response can be at root level or wrapped in 'content'
        # Handle both structures for compatibility
        data = event.get("content", event)

        activity = data.get("activity") or {}
        sleep = data.get("sleep") or {}

        payload = {
            ATTR_DAILY_GOAL: activity.get("minutesGoal"),
            ATTR_MINUTES_ACTIVE: activity.get("minutesActive"),
            ATTR_MINUTES_DAY_SLEEP: sleep.get("minutesDaySleep"),
            ATTR_MINUTES_NIGHT_SLEEP: sleep.get("minutesNightSleep"),
            # Calm minutes can be used as rest indicator
            ATTR_MINUTES_REST: sleep.get("minutesCalm"),
        }
        self._dispatch_tracker_event(
            TRACKER_HEALTH_OVERVIEW_UPDATED, data["petId"], payload
        )

    def _send_position_update(self, event: dict[str, Any]) -> None:
        payload = {
            "latitude": event["position"]["latlong"][0],
            "longitude": event["position"]["latlong"][1],
            "accuracy": event["position"]["accuracy"],
            "sensor_used": event["position"]["sensor_used"],
        }
        self._dispatch_tracker_event(
            TRACKER_POSITION_UPDATED, event["tracker_id"], payload
        )

    def _dispatch_tracker_event(
        self, event_name: str, tracker_id: str, payload: dict[str, Any]
    ) -> None:
        async_dispatcher_send(
            self._hass,
            f"{event_name}-{tracker_id}",
            payload,
        )
