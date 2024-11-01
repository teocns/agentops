from __future__ import annotations  # Allow forward references

import datetime as dt
import json
import queue
import threading
from dataclasses import asdict, dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import (
    Annotated,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)
from uuid import UUID, uuid4

from termcolor import colored

from .config import Configuration
from .enums import EndState, EventType
from .event import ErrorEvent, Event
from .exceptions import ApiServerException
from .helpers import filter_unjsonable, get_ISO_time, safe_serialize
from .http_client import HttpClient
from .log_config import logger

"""
Summary

1. **Imports and Dependencies**: The code imports various modules and packages, including threading, JSON handling, and HTTP client utilities. It also imports custom modules like `Configuration`, `EndState`, `ErrorEvent`, `Event`, and others.

2. **Data Classes**:
   - `EventsCounter`: A simple data class to keep track of different types of events.
   - `SessionStruct`: A data class that holds the structure of a session, including attributes like `session_id`, `config`, `end_state`, `event_counts`, and more.

3. **Protocols and Mixins**:
   - `_SessionProto`: A protocol defining the expected methods and attributes for a session, such as `asdict` and `is_running`.
   - `SessionApiMixin`: A mixin class providing methods to interact with the session API, including updating sessions, reauthorizing JWTs, starting sessions, and dispatching events.

4. **Session Class**:
   - Inherits from `SessionStruct` and `SessionApiMixin`.
   - Manages the lifecycle of a session, including starting, ending, and recording events.
   - Provides methods to set video URLs, add or set tags, and handle session cleanup.
   - Utilizes threading to manage event dispatching in the background.

5. **Event Dispatcher**:
   - `EventDisptcherThread`: A threading class responsible for publishing events to the API in the background.

6. **Utility Functions**:
   - `_serialize_batch`: A function to efficiently serialize a batch of events for transmission.

7. **Global Variables**:
   - `active_sessions`: A list to keep track of active session instances.
"""


"""

Major changes:

- Removed unsafe usage of __dict__ (which is very dangerous when used without __slots__)
"""


@dataclass
class EventsCounter:
    llms: int = 0
    tools: int = 0
    actions: int = 0
    errors: int = 0
    apis: int = 0


# TODO: Ideally, we can have a custom list with counters builtin
# class EventCounterList(list):
#     def __init__(self, *args):
#         super().__init__(*args)
#         self.counters = {event.name: 0 for event in EventType}


class Counter(object):
    """
    Thread safe Counter
    """

    # TODO: Move to a separate, utilities module

    def __init__(self, value=0, mutex=None):
        # RawValue because we don't need it to create a Lock:
        self.val = value
        self.mutex = mutex or threading.Lock()

    def increment(self):
        with self.mutex:
            self.val += 1

    def decrement(self):
        with self.mutex:
            self.val -= 1

    def value(self):
        with self.mutex:
            return self.val


@dataclass
class SessionStruct:
    session_id: UUID
    # --------------
    config: Configuration
    end_state: str = EndState.INDETERMINATE.value
    end_state_reason: Optional[str] = None
    end_timestamp: Optional[str] = None
    # Create a counter dictionary with each EventType name initialized to 0
    event_counts: Dict[str, int] = field(
        default_factory=lambda: {event.name: 0 for event in EventType}
    )  # Sets them to 0 by default
    host_env: Optional[dict] = None
    init_timestamp: str = field(default_factory=get_ISO_time)
    is_running: bool = False
    jwt: Optional[str] = None
    tags: Optional[List[str]] = None
    video: Optional[str] = None


@runtime_checkable
class _SessionProto(Protocol):
    """Protocol for internal Session attributes that shouldn't be part of the dataclass"""

    locks: Dict[Literal["lifecycle", "events", "session", "tags"], threading.Lock]
    config: Configuration
    session_id: UUID

    def asdict(self) -> Dict[str, Any]: ...

    def is_running(self) -> bool: ...


class SessionApi:
    """
    Solely focuses on interacting with the API

    Developer notes:
        Need to clarify (and define) a standard and consistent Api interface.

        The way it can be approached is by having a base `Api` class that holds common
        configurations and features, while implementors provide entity-related controllers
    """

    # TODO: Decouple from standard Configuration a Session's entity own configuration.
    # NOTE: pydantic-settings plays out beautifully in such setup, but it's not a requirement.
    # TODO: Eventually move to apis/
    session: Session

    def __init__(self, session: Session):
        self.session = session

    @property
    def config(self):  # Forward decl.
        return self.session.config

    def update_session(self) -> None:
        try:
            payload = {"session": asdict(self.session)}
            res = HttpClient.post(
                f"{self.config.endpoint}/v2/update_session",
                json.dumps(filter_unjsonable(payload)).encode("utf-8"),
                jwt=self.jwt,
            )
        except ApiServerException as e:
            return logger.error(f"Could not update session - {e}")

    # WARN: Unused method
    def reauthorize_jwt(self) -> Union[str, None]:
        payload = {"session_id": self.session.session_id}
        serialized_payload = json.dumps(filter_unjsonable(payload)).encode("utf-8")
        res = HttpClient.post(
            f"{self.config.endpoint}/v2/reauthorize_jwt",
            serialized_payload,
            self.config.api_key,
        )

        logger.debug(res.body)

        if res.code != 200:
            return None

        jwt = res.body.get("jwt", None)
        self.jwt = jwt
        return jwt

    def create_session(self, session: SessionStruct):
        """
        Creates a new session via API call

        Returns:
            tuple containing:
            - success (bool): Whether the creation was successful
            - jwt (Optional[str]): JWT token if successful
            - session_url (Optional[str]): URL to view the session if successful
        """
        payload = {"session": asdict(session)}
        serialized_payload = json.dumps(filter_unjsonable(payload)).encode("utf-8")

        try:
            res = HttpClient.post(
                f"{self.config.endpoint}/v2/create_session",
                serialized_payload,
                self.config.api_key,
                self.config.parent_key,
            )
        except ApiServerException as e:
            logger.error(f"Could not start session - {e}")
            return False
        else:
            if res.code != 200:
                return False

            jwt = res.body.get("jwt", None)
            self.jwt = jwt
            if jwt is None:
                return False

            session_url = res.body.get(
                "session_url",
                f"https://app.agentops.ai/drilldown?session_id={session.session_id}",
            )

            logger.info(
                colored(
                    f"\x1b[34mSession Replay: {session_url}\x1b[0m",
                    "blue",
                )
            )

            return True

    def batch(self, events: List[Event]) -> None:
        serialized_payload = safe_serialize(dict(events=events)).encode("utf-8")
        try:
            HttpClient.post(
                f"{self.config.endpoint}/v2/create_events",
                serialized_payload,
                jwt=self.jwt,
            )
        except ApiServerException as e:
            return logger.error(f"Could not post events - {e}")

        # Update event counts
        for event in events:
            event_type = event.get("event_type")
            if event_type in self.event_counts:
                self.event_counts[event_type] += 1

        logger.debug("\n<AGENTOPS_DEBUG_OUTPUT>")
        logger.debug(f"Session request to {self.config.endpoint}/v2/create_events")
        logger.debug(serialized_payload)
        logger.debug("</AGENTOPS_DEBUG_OUTPUT>\n")


class Session(SessionStruct):
    """
    Represents a session of events, with a start and end state.

    Args:
        session_id (UUID): The session id is used to record particular runs.
        tags (List[str], optional): Tags that can be used for grouping or sorting later. Examples could be ["GPT-4"].

    Attributes:
        init_timestamp (float): The timestamp for when the session started, represented as seconds since the epoch.
        end_timestamp (float, optional): The timestamp for when the session ended, represented as seconds since the epoch. This is only set after end_session is called.
        end_state (str, optional): The final state of the session. Suggested: "Success", "Fail", "Indeterminate". Defaults to "Indeterminate".
        end_state_reason (str, optional): The reason for ending the session.

    """

    # If ever wanting to safely use __dict__ for serialization, uncomment the below
    # __slots__ = [
    #     "session_id",
    #     "init_timestamp",
    #     "end_timestamp",
    #     "end_state",
    #     "end_state_reason",
    #     "tags",
    #     "video",
    #     "host_env",
    #     "config",
    #     "jwt",
    #     "event_counts",
    # ]

    thread: Annotated[
        EventPublisherThread,
        (
            "Publishes events to the API in a background thread."
            "TODO: an eventual async support release won't need a Thread; "
            "instead attach to existing loop executor. Plan for support"
        ),
    ]

    locks: Dict[Literal["lifecycle", "events", "session", "tags"], threading.Lock]
    cconditions: Dict[Literal["changes"], threading.Condition]

    def __init__(
        self,
        session_id: UUID,
        config: Configuration,
        tags: Optional[List[str]] = None,
        host_env: Optional[dict] = None,
    ):
        super().__init__(session_id=session_id, config=config, tags=tags, host_env=host_env)
        self._events = queue.Queue[Event](self.config.max_queue_size)
        self._cleanup_done = False
        
        # Initialize locks
        self.locks = {
            "lifecycle": threading.Lock(),  # Controls session lifecycle operations
            "events": threading.Lock(),     # Protects event queue operations
            "session": threading.Lock(),    # Protects session state updates
            "tags": threading.Lock(),       # Protects tag modifications
        }
        
        # Initialize conditions
        self.conditions = {
            "cleanup": threading.Condition(self.locks["lifecycle"]),
            "changes": threading.Condition(self.locks["session"]),
        }

        self.thread = EventPublisherThread(self)
        self.thread.start()

        self._is_running = False  # Protected state variable
        self.is_running = self._start_session()
        
        if not self.is_running:
            self.stop()

    @property 
    def is_running(self) -> bool:
        """Thread-safe access to running state"""
        with self.locks["lifecycle"]:
            return self._is_running

    @is_running.setter 
    def is_running(self, value: bool):
        """Thread-safe modification of running state"""
        with self.locks["lifecycle"]:
            self._is_running = value

    def set_video(self, video: str) -> None:
        """
        Sets a url to the video recording of the session.

        Args:
            video (str): The url of the video recording
        """
        self.video = video

    def add_tags(self, tags: List[str]) -> None:
        """
        Append to session tags at runtime.

        Args:
            tags (List[str]): The list of tags to append.
        """
        if not self.is_running:
            return

        if not (isinstance(tags, list) and all(isinstance(item, str) for item in tags)):
            if isinstance(tags, str):
                tags = [tags]

        if self.tags is None:
            self.tags = tags
        else:
            for tag in tags:
                if tag not in self.tags:
                    self.tags.append(tag)

        self._publish()

    def set_tags(self, tags):
        if not self.is_running:
            return

        if not (isinstance(tags, list) and all(isinstance(item, str) for item in tags)):
            if isinstance(tags, str):
                tags = [tags]

        self.tags = tags
        self._publish()

    # --- Interactors
    def record(self, event: Union[Event, ErrorEvent]):
        if not self.is_running:
            return
        if isinstance(event, Event):
            if not event.end_timestamp or event.init_timestamp == event.end_timestamp:
                event.end_timestamp = get_ISO_time()  # WARN: Unrestricted assignment
        elif isinstance(event, ErrorEvent):
            if event.trigger_event:
                if (
                    not event.trigger_event.end_timestamp
                    or event.trigger_event.init_timestamp
                    == event.trigger_event.end_timestamp
                ):
                    event.trigger_event.end_timestamp = get_ISO_time()

                event.trigger_event_id = event.trigger_event.id
                event.trigger_event_type = event.trigger_event.event_type
                self._enqueue(event.trigger_event.__dict__)
                event.trigger_event = None  # removes trigger_event from serialization
                # ^^ NOTE: Consider memento https://refactoring.guru/design-patterns/memento/python/example

        self._enqueue(
            event.__dict__
        )  # WARNING: This is very dangerous. Either define Event.__slots__ or turn Event into a dataclass

    def end_session(
        self,
        end_state: str = "Indeterminate",
        end_state_reason: Optional[str] = None,
        video: Optional[str] = None,
    ) -> Union[Decimal, None]:
        if not self.is_running:
            raise RuntimeError("Cannot end a terminated session")

        if not any(end_state == state.value for state in EndState):
            return logger.warning(
                "Invalid end_state. Please use one of the EndState enums"
            )

        self.end_state = end_state or self.end_state
        self.end_state_reason = end_state_reason or self.end_state_reason

        # TODO: Privatize modifier by nomenclature
        def __duration():
            start = dt.datetime.fromisoformat(
                self.init_timestamp.replace("Z", "+00:00")
            )
            end = dt.datetime.fromisoformat(end_timestamp.replace("Z", "+00:00"))
            duration = end - start

            hours, remainder = divmod(duration.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)

            parts = []
            if hours > 0:
                parts.append(f"{int(hours)}h")
            if minutes > 0:
                parts.append(f"{int(minutes)}m")
            parts.append(f"{seconds:.1f}s")

            return " ".join(parts)

        with self.locks["api"]:
            payload = {"session": self.__dict__}  # WARNING: This is very dangerous
            try:
                res = HttpClient.post(
                    f"{self.config.endpoint}/v2/update_session",
                    json.dumps(filter_unjsonable(payload)).encode("utf-8"),
                    jwt=self.jwt,
                )
            except ApiServerException as e:
                return logger.error(f"Could not end session - {e}")

        logger.debug(res.body)
        token_cost = res.body.get("token_cost", "unknown")

        formatted_duration = __duration()

        if token_cost == "unknown" or token_cost is None:
            token_cost_d = Decimal(0)
        else:
            token_cost_d = Decimal(token_cost)

        formatted_cost = (
            "{:.2f}".format(token_cost_d)
            if token_cost_d == 0
            else "{:.6f}".format(
                token_cost_d.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            )
        )

        analytics = (
            f"Session Stats - "
            f"{colored('Duration:', attrs=['bold'])} {formatted_duration} | "
            f"{colored('Cost:', attrs=['bold'])} ${formatted_cost} | "
            f"{colored('LLMs:', attrs=['bold'])} {self.event_counts['llms']} | "
            f"{colored('Tools:', attrs=['bold'])} {self.event_counts['tools']} | "
            f"{colored('Actions:', attrs=['bold'])} {self.event_counts['actions']} | "
            f"{colored('Errors:', attrs=['bold'])} {self.event_counts['errors']}"
        )
        logger.info(analytics)

        session_url = res.body.get(
            "session_url",
            f"https://app.agentops.ai/drilldown?session_id={self.session_id}",
        )

        logger.info(
            colored(
                f"\x1b[34mSession Replay: {session_url}\x1b[0m",
                "blue",
            )
        )

        active_sessions.remove(self)

        return token_cost_d

    def create_agent(
        self, name: str, agent_id: Optional[str] = None
    ) -> object:  # FIXME: Is this `int`, `UUID`, or `str`?
        if not self.is_running:
            return
        if agent_id is None:
            agent_id = str(uuid4())

        payload = {
            "id": agent_id,
            "name": name,
        }

        serialized_payload = safe_serialize(payload).encode("utf-8")
        try:
            HttpClient.post(
                f"{self.config.endpoint}/v2/create_agent",
                serialized_payload,
                jwt=self.jwt,
            )
        except ApiServerException as e:
            logger.error(f"Could not create agent - {e}")

        return agent_id

    def _enqueue(self, event: dict) -> None:
        # with self.events_buffer.mutex:
        self._events.queue.append(event)

        if len(self._events) >= self.config.max_queue_size:
            self._flush_queue()

    def _publish(self):
        """Notify the ChangesObserverThread to perform the API call."""
        self.conditions["changes"].notify()

    def stop(self) -> None:
        """
        Stops the session and initiates cleanup.
        Thread-safe and idempotent.
        """
        with self.locks["lifecycle"]:
            if not self._is_running:
                return
            
            self._is_running = False
            
            # Flush any remaining events
            with self.locks["events"]:
                if not self._events.empty():
                    self._flush_queue()
            
            # Signal the publisher thread to stop
            self.thread.stop()
            
            # Wait for thread cleanup with timeout from config
            self.thread.join(timeout=self.config.graceful_shutdown_wait_time / 1000)
            
            # Ensure session is properly ended
            if not self.end_timestamp:
                try:
                    self.end_session(
                        end_state="Indeterminate",
                        end_state_reason="Session terminated during cleanup",
                    )
                except Exception as e:
                    logger.error(f"Failed to end session during cleanup: {e}")
            
            with self.conditions["cleanup"]:
                self._cleanup_done = True
                self.conditions["cleanup"].notify_all()

    def _cleanup(self) -> None:
        """
        Internal cleanup method that ensures proper session termination.
        Can be called multiple times safely.
        """
        with self.conditions["cleanup"]:
            if self._cleanup_done:
                return
            
            self.stop()
            
            # Wait for cleanup to complete with configured timeout
            self.conditions["cleanup"].wait(
                timeout=self.config.graceful_shutdown_wait_time / 1000
            )

    def __del__(self):
        """Ensure cleanup runs when object is garbage collected"""
        try:
            self._cleanup()
        except Exception as e:
            logger.error(f"Error during session cleanup: {e}")

    def _flush_queue(self) -> None:
        """Thread-safe queue flushing"""
        with self.locks["events"]:
            events = []
            while not self._events.empty():
                try:
                    events.append(self._events.get_nowait())
                except queue.Empty:
                    break
            
            if events:
                try:
                    self.batch(events)
                except Exception as e:
                    logger.error(f"Failed to batch events during flush: {e}")


class _SessionThread(threading.Thread):
    """Base class for session-related threads."""

    def __init__(self, session: Session):
        super().__init__()
        self.s = session
        self.daemon = True
        self.l_stop = threading.Lock()

    @property
    def stopping(self) -> bool:
        return self.l_stop.locked()

    @property
    def running(self) -> bool:
        return not self.stopping

    def stop(self) -> None:
        with self.stopping:
            with self.s.runtime_condition:
                if not self.s._events:
                    self.s.runtime_condition.wait(
                        timeout=self.s.config.max_wait_time / 1000
                    )
                if self.s._events:
                    self.s._flush_queue()


class ChangesObserverThread(_SessionThread):
    """Observes changes in the session and performs API calls for event publishing."""

    def run(self) -> None:
        """
        Waits for a condition and performs API calls to publish events.
        """
        while self.running:
            with (condition := self.s.conditions["changes"]):
                condition.wait()  # Wait for a notification from _publish

                self._perform_api_call()  # Perform the API call to publish events using SessionApi

    def _perform_api_call(self) -> None:
        """
        Performs the API call using SessionApi.
        """
        try:
            # Example API call
            self.session.api.update_session()
            logger.info("Session updated successfully.")
        except ApiServerException as e:
            logger.error(f"Could not update session - {e}")


class EventPublisherThread(_SessionThread):
    """Polls events from Session, publishes API batches"""

    @property
    def feed(self) -> queue.Queue:
        return self.s._events

    def run(self) -> None:  # virtual override
        """
        `threading.Thread` invokes this on Thread.start()

        Will poll for events
        """
        while True:
            with self.poll:
                if not self.s._events:
                    self.s.runtime_condition.wait(
                        timeout=self.s.config.max_wait_time / 1000
                    )
                if self.s._events:
                    self.s._flush_queue()


# def _serialize_batch(self, events: List[dict]) -> bytes:
#     """
#     Efficiently serialize a batch of events.
#
#     Args:
#         events (List[dict]): List of event dictionaries to serialize
#
#     Returns:
#         bytes: Serialized events payload ready for transmission
#     """
#     payload = {
#         "events": events,
#         "session_id": str(self.session_id),
#         "batch_size": len(events)
#     }
#
#     # Pre-process the events to remove unwanted fields
#     for event in events:
#         if "trigger_event" in event:
#             # Handle trigger events specially to avoid circular references
#             trigger = event["trigger_event"]
#             if trigger:
#                 event["trigger_event_id"] = trigger.get("id")
#                 event["trigger_event_type"] = trigger.get("event_type")
#                 del event["trigger_event"]
#
#     return safe_serialize(payload).encode("utf-8")


active_sessions: List[Session] = []

__all__ = ["Session"]
