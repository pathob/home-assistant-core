"""Test init of Tractive integration."""

import asyncio
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

from aiotractive.exceptions import TractiveError, UnauthorizedError
from freezegun.api import FrozenDateTimeFactory
import pytest

from homeassistant.components.sensor import DOMAIN as SENSOR_DOMAIN
from homeassistant.components.tractive.const import (
    ATTR_DAILY_GOAL,
    ATTR_MINUTES_ACTIVE,
    ATTR_MINUTES_DAY_SLEEP,
    ATTR_MINUTES_NIGHT_SLEEP,
    ATTR_MINUTES_REST,
    DOMAIN,
    SERVER_UNAVAILABLE,
    UNAVAILABLE_AFTER,
)
from homeassistant.config_entries import SOURCE_REAUTH, ConfigEntryState
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from . import init_integration

from tests.common import MockConfigEntry, async_fire_time_changed, async_mock_signal

# The real sleep, captured before the mock_tractive_client fixture patches it
REAL_SLEEP = asyncio.sleep

TRACKER_ENTITY_ID = "device_tracker.tracker_device_id_123"
BATTERY_ENTITY_ID = "sensor.tracker_device_id_123_battery"
DAILY_GOAL_ENTITY_ID = "sensor.test_pet_daily_goal"
CHARGING_ENTITY_ID = "binary_sensor.tracker_device_id_123_charging"
BUZZER_ENTITY_ID = "switch.tracker_device_id_123_buzzer"
ENTITY_IDS = (
    TRACKER_ENTITY_ID,
    BATTERY_ENTITY_ID,
    DAILY_GOAL_ENTITY_ID,
    CHARGING_ENTITY_ID,
    BUZZER_ENTITY_ID,
)

# The channel opens with one of these per tracker, carrying the last known state
SNAPSHOT_EVENT = {
    "message": "tracker_status",
    "tracker_id": "device_id_123",
    "tracker_state": "OPERATIONAL",
    "tracker_state_reason": "POWER_SAVING",
    "charging_state": "NOT_CHARGING",
    "hardware": {"time": 1716106600, "battery_level": 75},
    "position": {
        "time": 1716106600,
        "latlong": [11.111, 22.222],
        "accuracy": 30,
        "sensor_used": "KNOWN_WIFI",
    },
    "buzzer_control": {"active": True},
}


async def _channel(
    reconnect: asyncio.Event,
    connected: asyncio.Event,
    drop: asyncio.Event | None = None,
) -> AsyncGenerator[dict[str, Any]]:
    """Event channel that stays down until reconnect is set.

    It then opens with the tracker status and stays silent, or raises once
    drop is set.
    """
    await reconnect.wait()
    yield SNAPSHOT_EVENT
    connected.set()
    if drop is None:
        await asyncio.get_running_loop().create_future()
    await drop.wait()
    raise TractiveError


async def _wait_for(condition: Callable[[], bool]) -> None:
    """Yield to the listener task until the condition holds."""
    for _ in range(1000):
        if condition():
            return
        await REAL_SLEEP(0)
    pytest.fail("Condition not met")


async def _init_with_state(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Set up the integration with every entity available."""
    await init_integration(hass, mock_config_entry)
    mock_tractive_client.send_hardware_event(mock_config_entry)
    mock_tractive_client.send_position_event(mock_config_entry)
    mock_tractive_client.send_switch_event(mock_config_entry)
    await hass.async_block_till_done()
    for entity_id in ENTITY_IDS:
        assert hass.states.get(entity_id).state != STATE_UNAVAILABLE


async def _advance(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, delta: timedelta
) -> None:
    freezer.tick(delta)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()


async def _drop_channel(
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    *channels: AsyncGenerator[dict[str, Any]],
) -> None:
    """Start the listener on a dropped channel and wait for the next connect."""
    mock_tractive_client.events.side_effect = [TractiveError, *channels]
    mock_config_entry.runtime_data.client.subscribe()
    await _wait_for(lambda: mock_tractive_client.events.call_count == 2)


async def test_setup_entry(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Test a successful setup entry."""
    await init_integration(hass, mock_config_entry)

    assert mock_config_entry.state is ConfigEntryState.LOADED


async def test_unload_entry(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Test successful unload of entry."""
    await init_integration(hass, mock_config_entry)

    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert mock_config_entry.state is ConfigEntryState.LOADED

    with patch("homeassistant.components.tractive.TractiveClient.unsubscribe"):
        assert await hass.config_entries.async_unload(mock_config_entry.entry_id)
        await hass.async_block_till_done()

    assert mock_config_entry.state is ConfigEntryState.NOT_LOADED
    assert not hass.data.get(DOMAIN)


@pytest.mark.parametrize(
    ("method", "exc", "entry_state"),
    [
        ("authenticate", UnauthorizedError, ConfigEntryState.SETUP_ERROR),
        ("authenticate", TractiveError, ConfigEntryState.SETUP_RETRY),
        ("trackable_objects", TractiveError, ConfigEntryState.SETUP_RETRY),
    ],
)
async def test_setup_failed(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    method: str,
    exc: Exception,
    entry_state: ConfigEntryState,
) -> None:
    """Test for setup failure."""
    getattr(mock_tractive_client, method).side_effect = exc

    await init_integration(hass, mock_config_entry)

    assert mock_config_entry.state is entry_state


async def test_config_not_ready(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Test for setup failure if the tracker_details doesn't contain '_id'."""
    mock_tractive_client.tracker.return_value.details.return_value.pop("_id")

    await init_integration(hass, mock_config_entry)

    assert mock_config_entry.state is ConfigEntryState.SETUP_RETRY


async def test_trackable_without_details(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test a successful setup entry."""
    mock_tractive_client.trackable_objects.return_value[0].details.return_value = {
        "device_id": "xyz098"
    }

    await init_integration(hass, mock_config_entry)

    assert (
        "Tracker xyz098 has no details and will be skipped."
        " This happens for shared trackers" in caplog.text
    )
    assert mock_config_entry.state is ConfigEntryState.LOADED


async def test_trackable_without_device_id(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Test a successful setup entry."""
    mock_tractive_client.trackable_objects.return_value[0].details.return_value = {
        "device_id": None
    }

    await init_integration(hass, mock_config_entry)

    assert mock_config_entry.state is ConfigEntryState.LOADED


async def test_unsubscribe_on_ha_stop(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Test unsuscribe when HA stops."""
    await init_integration(hass, mock_config_entry)

    with patch(
        "homeassistant.components.tractive.TractiveClient.unsubscribe"
    ) as mock_unsuscribe:
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
        await hass.async_block_till_done()

    assert mock_unsuscribe.called


async def test_server_unavailable(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Test states of the sensor."""
    entity_id = "sensor.tracker_device_id_123_battery"

    await init_integration(hass, mock_config_entry)

    # send event to make the entity available
    mock_tractive_client.send_hardware_event(mock_config_entry)
    await hass.async_block_till_done()

    assert hass.states.get(entity_id).state != STATE_UNAVAILABLE

    # send server unavailable event, the entity should be unavailable
    mock_tractive_client.send_server_unavailable_event(hass)
    await hass.async_block_till_done()

    assert hass.states.get(entity_id).state == STATE_UNAVAILABLE

    # send event to make the entity available once again
    mock_tractive_client.send_hardware_event(mock_config_entry)
    await hass.async_block_till_done()

    assert hass.states.get(entity_id).state != STATE_UNAVAILABLE


@pytest.mark.parametrize(("sleep_data"), [None, {}, {"unexpected": 123}])
async def test_missing_sleep_data(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    sleep_data: dict[str, Any] | None,
) -> None:
    """Test for missing sleep data."""
    event = {"petId": "pet_id_123", "sleep": sleep_data}

    await init_integration(hass, mock_config_entry)

    with patch(
        "homeassistant.components.tractive.async_dispatcher_send"
    ) as async_dispatcher_send_mock:
        mock_tractive_client.send_health_overview_event(mock_config_entry, event)

    assert async_dispatcher_send_mock.call_count == 1
    payload = async_dispatcher_send_mock.mock_calls[0][1][2]
    assert payload[ATTR_MINUTES_DAY_SLEEP] is None
    assert payload[ATTR_MINUTES_NIGHT_SLEEP] is None
    assert payload[ATTR_MINUTES_REST] is None


@pytest.mark.parametrize(("activity_data"), [None, {}, {"unexpected": 123}])
async def test_missing_activity_data(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    activity_data: dict[str, Any] | None,
) -> None:
    """Test for missing activity data."""
    event = {"petId": "pet_id_123", "activity": activity_data}

    await init_integration(hass, mock_config_entry)

    with patch(
        "homeassistant.components.tractive.async_dispatcher_send"
    ) as async_dispatcher_send_mock:
        mock_tractive_client.send_health_overview_event(mock_config_entry, event)

    assert async_dispatcher_send_mock.call_count == 1
    payload = async_dispatcher_send_mock.mock_calls[0][1][2]
    assert payload[ATTR_DAILY_GOAL] is None
    assert payload[ATTR_MINUTES_ACTIVE] is None


@pytest.mark.parametrize("sensor", ["activity_label", "calories", "sleep_label"])
async def test_remove_unsupported_sensor_entity(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    entity_registry: er.EntityRegistry,
    sensor: str,
) -> None:
    """Test removing unsupported sensor entity."""
    entity_id = f"sensor.test_pet_{sensor}"
    mock_config_entry.add_to_hass(hass)

    entity_registry.async_get_or_create(
        SENSOR_DOMAIN,
        DOMAIN,
        f"pet_id_123_{sensor}",
        suggested_object_id=entity_id.rsplit(".", maxsplit=1)[-1],
        config_entry=mock_config_entry,
    )

    await init_integration(hass, mock_config_entry)

    assert entity_registry.async_get(entity_id) is None


@pytest.mark.parametrize(
    ("outage", "unavailable", "health_overview_fetches"),
    [
        pytest.param(
            UNAVAILABLE_AFTER - timedelta(seconds=1), False, 1, id="within_grace_period"
        ),
        pytest.param(UNAVAILABLE_AFTER, True, 2, id="grace_period_exceeded"),
    ],
)
async def test_outage_grace_period(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    freezer: FrozenDateTimeFactory,
    outage: timedelta,
    unavailable: bool,
    health_overview_fetches: int,
) -> None:
    """Test entities go unavailable only after the grace period and recover.

    The reconnect hangs without raising, as aiotractive does on connect timeouts.
    """
    await _init_with_state(hass, mock_tractive_client, mock_config_entry)
    health_overview = mock_tractive_client.trackable_object.return_value.health_overview
    health_overview.return_value = {
        "petId": "pet_id_123",
        "activity": {"minutesGoal": 250},
    }
    reconnect = asyncio.Event()
    connected = asyncio.Event()
    await _drop_channel(
        mock_tractive_client, mock_config_entry, _channel(reconnect, connected)
    )

    await _advance(hass, freezer, outage)

    for entity_id in ENTITY_IDS:
        assert (hass.states.get(entity_id).state == STATE_UNAVAILABLE) is unavailable

    reconnect.set()
    await _wait_for(connected.is_set)
    await hass.async_block_till_done()

    for entity_id in ENTITY_IDS:
        assert hass.states.get(entity_id).state != STATE_UNAVAILABLE
    # The tracker state comes from the channel, not from polling the REST API
    assert mock_tractive_client.tracker.return_value.details.await_count == 1
    assert hass.states.get(BATTERY_ENTITY_ID).state == "75"
    assert hass.states.get(TRACKER_ENTITY_ID).attributes["latitude"] == 11.111
    assert hass.states.get(CHARGING_ENTITY_ID).state == "off"
    assert hass.states.get(BUZZER_ENTITY_ID).state == "on"
    # The channel does not replay the health overview, so it is fetched after
    # an outage long enough to have marked it unavailable
    assert health_overview.await_count == health_overview_fetches


async def test_outage_timer_resets_on_reconnect(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Test a reconnect cancels the timer and a later outage gets its own."""
    await _init_with_state(hass, mock_tractive_client, mock_config_entry)
    reconnect = asyncio.Event()
    connected = asyncio.Event()
    drop = asyncio.Event()
    await _drop_channel(
        mock_tractive_client,
        mock_config_entry,
        _channel(reconnect, connected, drop),
        _channel(asyncio.Event(), asyncio.Event()),
    )
    short_outage = UNAVAILABLE_AFTER - timedelta(minutes=1)
    await _advance(hass, freezer, short_outage)
    reconnect.set()
    await _wait_for(connected.is_set)

    # Past when the first outage's timer would have fired
    await _advance(hass, freezer, short_outage)
    assert hass.states.get(BATTERY_ENTITY_ID).state != STATE_UNAVAILABLE

    drop.set()
    await _wait_for(lambda: mock_tractive_client.events.call_count == 3)
    await _advance(hass, freezer, short_outage)
    assert hass.states.get(BATTERY_ENTITY_ID).state != STATE_UNAVAILABLE

    await _advance(hass, freezer, UNAVAILABLE_AFTER - short_outage)
    assert hass.states.get(BATTERY_ENTITY_ID).state == STATE_UNAVAILABLE


async def test_unload_during_outage_cancels_timer(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Test unloading during an outage does not leave the timer behind."""
    await _init_with_state(hass, mock_tractive_client, mock_config_entry)
    server_unavailable = async_mock_signal(hass, f"{SERVER_UNAVAILABLE}-12345")
    await _drop_channel(
        mock_tractive_client,
        mock_config_entry,
        _channel(asyncio.Event(), asyncio.Event()),
    )

    assert await hass.config_entries.async_unload(mock_config_entry.entry_id)
    await _advance(hass, freezer, UNAVAILABLE_AFTER)

    assert server_unavailable == []


async def test_health_overview_refresh_failure_keeps_channel(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Test a failed health overview refresh does not drop a working channel."""
    await _init_with_state(hass, mock_tractive_client, mock_config_entry)
    mock_tractive_client.trackable_object.return_value.health_overview.side_effect = (
        TractiveError
    )
    reconnect = asyncio.Event()
    connected = asyncio.Event()
    await _drop_channel(
        mock_tractive_client, mock_config_entry, _channel(reconnect, connected)
    )
    await _advance(hass, freezer, UNAVAILABLE_AFTER)

    reconnect.set()
    await _wait_for(connected.is_set)
    await hass.async_block_till_done()

    assert mock_tractive_client.events.call_count == 2
    assert hass.states.get(BATTERY_ENTITY_ID).state == "75"
    # Stays unavailable until the channel pushes the next health overview
    assert hass.states.get(DAILY_GOAL_ENTITY_ID).state == STATE_UNAVAILABLE


async def test_unauthorized_during_health_overview_refresh_starts_reauth(
    hass: HomeAssistant,
    mock_tractive_client: AsyncMock,
    mock_config_entry: MockConfigEntry,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Test a rejected token during the health overview refresh starts reauth."""
    await _init_with_state(hass, mock_tractive_client, mock_config_entry)
    mock_tractive_client.trackable_object.return_value.health_overview.side_effect = (
        UnauthorizedError
    )
    reconnect = asyncio.Event()
    await _drop_channel(
        mock_tractive_client,
        mock_config_entry,
        _channel(reconnect, asyncio.Event()),
    )
    await _advance(hass, freezer, UNAVAILABLE_AFTER)
    client = mock_config_entry.runtime_data.client

    reconnect.set()
    await _wait_for(lambda: not client.subscribed)
    await hass.async_block_till_done()

    flows = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert len(flows) == 1
    assert flows[0]["context"]["source"] == SOURCE_REAUTH
