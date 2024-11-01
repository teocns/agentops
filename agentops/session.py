from __future__ import annotations  # Allow forward references

import atexit
import copy
import datetime as dt
import functools
import json
import queue
import threading
import time
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import (Annotated, Any, ClassVar, Dict, Generator, List, Literal,
                    Optional, Protocol, Type, Union, runtime_checkable)
from uuid import UUID, uuid4
from warnings import deprecated

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




@dataclass
class EventsCounter:
    llms: int = 0
    tools: int = 0
    actions: int = 0
    errors: int = 0
    apis: int = 0


@dataclass
class SessionStruct:
    session_id: UUID
    # --------------
    config: Configuration
    end_state: str = EndState.INDETERMINATE.value
    end_state_reason: Optional[str] = None
    end_timestamp: Optional[str] = None
    # Create a counter dictionary with each EventType name initialized to 0
    event_counts: Dict[str, int] = field(default_factory=lambda: {event.name: 0 for event in EventType}) # Sets them to 0 by default
    host_env: Optional[dict] = None
    init_timestamp: str = field(default_factory=get_ISO_time)
    is_running: bool = False
    jwt: Optional[str] = None
    tags: Optional[List[str]] = None
    video: Optional[str] = None

@runtime_checkable
class _SessionProto(Protocol):
    """Protocol for internal Session attributes that shouldn't be part of the dataclass"""
    locks: Dict[Literal['lifecycle', 'events', 'session', 'tags'], threading.Lock]
    config: Configuration
    session_id: UUID

    def asdict(self) -> Dict[str, Any]:
        ...
    def is_running(self) -> bool:
        ...


        

class SessionApiMixin(_SessionProto):
    """Focuses exclusively on interacting with the API"""

    # FIXME: Need to clarify or define a standardized Api interface. Priorily, these 
    # methods used to perform something like `return logger.error(exc)` within 
    # the catch blocks`, which wasn't intuitive enough to define expectancies

    def _update_session(self) -> None:
        try:
            payload = {"session": self.asdict()}
            res = HttpClient.post(
                f"{self.config.endpoint}/v2/update_session",
                json.dumps(filter_unjsonable(payload)).encode("utf-8"),
                jwt=self.jwt,
            )
        except ApiServerException as e:
            return logger.error(f"Could not update session - {e}")

    def _reauthorize_jwt(self) -> Union[str, None]:
        payload = {"session_id": self.session_id}
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

    def _start_session(self) -> bool:
        with self.locks['lifecycle']:
            payload = {"session": self.__dict__}
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

            logger.debug(res.body)

            if res.code != 200:
                return False

            jwt = res.body.get("jwt", None)
            self.jwt = jwt
            if jwt is None:
                return False

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

            return True

    def dispatch(self, session: SessionStruct, events: List[dict]) -> None:
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

    def create_agent(self, name: str, agent_id: Optional[str] = None) -> None:
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

class Session(SessionStruct, SessionApiMixin):
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

    def __init__(self, **kwargs):
        pass

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


    thread: Annotated[EventDisptcherThread, ("Publishes events to the API in a background thread."
                                             "TODO: an eventual async support release won't need a Thread; "
                                             "instead attach to existing loop executor. Plan for support")]

    locks: Dict[Literal['lifecycle', 'events', 'session', 'tags'], threading.Lock]


    def __init__(
        self,
        session_id: UUID,
        config: Configuration,
        tags: Optional[List[str]] = None,
        host_env: Optional[dict] = None,
    ):
        self.packet = None
        self.jwt = None
        
        self._events = queue.Queue[Event](self.config.max_queue_size)
        
        self.locks = {}
        for k in {'lifecycle', 'events', 'session', 'tags'}:
            self.locks[k] = threading.Lock()

        self.thread = EventDisptcherThread(self)
        self.thread.start()

        self.is_running = self._start_session()
        if self.is_running == False:
            self.stop_flag.set()
            self.thread.join(timeout=1)

    def set_video(self, video: str) -> None:
        """
        Sets a url to the video recording of the session.

        Args:
            video (str): The url of the video recording
        """
        self.video = video



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
            start = dt.datetime.fromisoformat(self.init_timestamp.replace("Z", "+00:00"))
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

        with self.locks['api']:
            payload = {"session": self.__dict__} # WARNING: This is very dangerous
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

        self._update_session()

    def set_tags(self, tags):
        if not self.is_running:
            return

        if not (isinstance(tags, list) and all(isinstance(item, str) for item in tags)):
            if isinstance(tags, str):
                tags = [tags]

        self.tags = tags
        self._update_session()

    def record(self, event: Union[Event, ErrorEvent]):
        if not self.is_running:
            return
        if isinstance(event, Event):
            if not event.end_timestamp or event.init_timestamp == event.end_timestamp:
                event.end_timestamp = get_ISO_time()
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

        self._enqueue(event.__dict__) # WARNING: This is very dangerous

    def _enqueue(self, event: dict) -> None:
        # with self.events_buffer.mutex:
        self._events.queue.append(event)
        
        if len(self._events) >= self.config.max_queue_size:
            self._flush_queue()
        self.condition.notify()


    @property
    def is_running(self):
        # Use the runtime condition to determine wheter we're running
        try:
            # If we can acquire it, then the session is NOT running
            return not self.runtime_condition.acquire(blocking=False)
        finally:
            # Release it immediately
            self.runtime_condition.release()

    def _cleanup(self):
        """
        Ensure the cleanup of the session.

        This method can run once per runtime to ensure that the session is properly cleaned up.
        Once the cleanup_event has occurred, it can be assumed that the session is no longer running.
        """
        # :: Try to acquire the runtime lock, but don't Block if it is not free
        # :: There is no reason to block because there's no reason to stack such events
        if not self.runtime_condition.acquire(blocking=False):
            # We can't perform a cleanup if there's no runtime
            # return self.thread.join()
            pass
        try:
            # if not self.
            # if self.is_running and not self.end_timestamp: # Why do we need to check for the end_timestamp though?
            # self.stop_flag.set() # FIXME: Does it need to show running?
            # self.thread.join(timeout=0.1)

            self.thread

            self.dispatch()

            try:
                self.terminate(
                    end_state="Indeterminate",
                    end_state_reason="Session interrupted",
                )
            except:
                pass

            self.cleanup_condition.notify()

            # self._cleanup_done = True
            # self.is_running = False
        finally:
            if acquired:
                self.condition.release()

class EventDisptcherThread(threading.Thread):
    """Thread to publish events to the API"""

    def __init__(self, session: Session):
        self.s = session
        self.daemon = True
        # self.empty_condition = threading.Condition()  # Notiies when the queue is empty
        self.stop_requested = threading.Lock()

    @property
    def feed(self) -> queue.Queue:
        return self.s._events

    @property
    def stopping(self) -> bool:
        return self.stop_requested.locked()

    @property
    def running(self) -> bool:
        return not self.stopping

    def stop(self) -> None:
        with self.stop_requested:
            with self.s.runtime_condition:
                if not self.s._events:
                    self.s.runtime_condition.wait(
                        timeout=self.s.config.max_wait_time / 1000
                    )
                if self.s._events:
                    self.s._flush_queue()

    def run(self) -> None:
        while self.running:
            with self.condition:
                # if not self.events_buffer:
                #     self.condition.wait(timeout=self.config.max_wait_time / 1000)
                # if self.events_buffer:
                #     self._flush_queue()



def _serialize_batch(self, events: List[dict]) -> bytes:
    """
    Efficiently serialize a batch of events.
    
    Args:
        events (List[dict]): List of event dictionaries to serialize
        
    Returns:
        bytes: Serialized events payload ready for transmission
    """
    payload = {
        "events": events,
        "session_id": str(self.session_id),
        "batch_size": len(events)
    }
    
    # Pre-process the events to remove unwanted fields
    for event in events:
        if "trigger_event" in event:
            # Handle trigger events specially to avoid circular references
            trigger = event["trigger_event"]
            if trigger:
                event["trigger_event_id"] = trigger.get("id")
                event["trigger_event_type"] = trigger.get("event_type")
                del event["trigger_event"]
    
    return safe_serialize(payload).encode("utf-8")


active_sessions: List[Session] = []

__all__ = [
    "Session"
]
