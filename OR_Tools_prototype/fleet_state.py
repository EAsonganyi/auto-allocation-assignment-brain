"""
Fleet State - Abstract Map for Real-Time Fleet Management

This module maintains an in-memory representation of the entire fleet:
- All agents with their real-time locations and projected locations
- All tasks (unassigned, in-progress, completed)
- Proximity monitoring for smart optimization triggers
- Eligibility caching for efficient decision making

The abstract map enables:
1. Radius-based assignment triggers (only optimize when agent is near task)
2. Proactive chaining (assign next task before agent finishes current)
3. Incremental optimization (optimize for one agent, not entire fleet)
4. Smart eligibility checks (pre-filter before expensive optimization)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any, ClassVar, Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
import threading
import math
import time
import logging
import os
import re

from region_policy import evaluate_region_eligibility, matching_regions, normalize_region


def _parse_datetime(dt_str: str) -> datetime:
    """
    Parse datetime from various formats:
    - ISO: '2025-12-18T10:04:00.000Z' or '2025-12-18T10:04:00+00:00'
    - US format: '12/18/2025 10:04 am' or '12/18/2025 10:04 AM'
    - Simple: '2025-12-18 10:04:00'
    """
    if not dt_str:
        return datetime.now(timezone.utc)
    
    # Try ISO format first (most common from API)
    try:
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str)
    except ValueError:
        pass
    
    # Try US format: '12/18/2025 10:04 am'
    try:
        # Match pattern: MM/DD/YYYY HH:MM am/pm
        match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)?', dt_str)
        if match:
            month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
            hour, minute = int(match.group(4)), int(match.group(5))
            ampm = match.group(6)
            
            # Handle AM/PM
            if ampm and ampm.lower() == 'pm' and hour != 12:
                hour += 12
            elif ampm and ampm.lower() == 'am' and hour == 12:
                hour = 0
            
            return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        pass
    
    # Fallback: return current time
    logger.warning(f"[FleetState] Could not parse datetime: {dt_str}, using current time")
    return datetime.now(timezone.utc)


def _normalize_tag_list(raw) -> List[str]:
    """Normalize tags from string, list, or Tookan {tag_name} objects."""
    if not raw:
        return []
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(',') if t.strip()]
    if isinstance(raw, (list, tuple)):
        tags = []
        for item in raw:
            if isinstance(item, dict):
                item = item.get('tag_name') or item.get('name') or ''
            tag = str(item).strip()
            if tag:
                tags.append(tag)
        return tags
    return []


def _extract_incoming_task_tags(task_data: Dict[str, Any], meta: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Build the assignment tag list from top-level tags, _meta.tags, and Task_Category.
    Task_Category (HEAVY, EV Scooter, etc.) is how the dashboard marks tagged tasks.
    """
    meta = meta or {}
    tags = _normalize_tag_list(task_data.get('tags'))
    tags.extend(_normalize_tag_list(meta.get('tags')))

    custom_fields = task_data.get('custom_fields') or meta.get('custom_fields') or {}
    if isinstance(custom_fields, dict):
        category = custom_fields.get('Task_Category') or custom_fields.get('task_category')
        if isinstance(category, str) and category.strip():
            tags.append(category.strip())
        tags.extend(_normalize_tag_list(custom_fields.get('tags')))

    seen = set()
    deduped = []
    for tag in tags:
        key = tag.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(tag)
    return deduped

# Configure logging
logging.basicConfig(level=logging.INFO)
# Use explicit name 'fleet_state' to match app.py's file handler connection
logger = logging.getLogger('fleet_state')


class AgentStatus(Enum):
    IDLE = "idle"      # No tasks, ready for work
    BUSY = "busy"      # Has tasks but available for more
    AT_CAPACITY = "at_capacity"  # Max tasks reached
    OFFLINE = "offline"  # Not working


class TaskStatus(Enum):
    UNASSIGNED = "unassigned"
    ASSIGNED = "assigned"
    PICKUP_IN_PROGRESS = "pickup_in_progress"
    DELIVERY_IN_PROGRESS = "delivery_in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class Location:
    lat: float
    lng: float
    
    def to_tuple(self) -> Tuple[float, float]:
        return (self.lat, self.lng)
    
    def distance_to(self, other: 'Location') -> float:
        """Calculate Haversine distance in km"""
        R = 6371  # Earth's radius in km
        lat1, lon1 = math.radians(self.lat), math.radians(self.lng)
        lat2, lon2 = math.radians(other.lat), math.radians(other.lng)
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def __str__(self):
        return f"({self.lat:.6f}, {self.lng:.6f})"


@dataclass
class TaskState:
    PREMIUM_TIP_THRESHOLD: ClassVar[float] = 5.0
    PREMIUM_DELIVERY_FEE_THRESHOLD: ClassVar[float] = 18.0
    id: str
    job_type: str  # "PAIRED", "PICKUP", "DELIVERY"
    restaurant_location: Location
    delivery_location: Location
    pickup_before: datetime
    delivery_before: datetime
    status: TaskStatus = TaskStatus.UNASSIGNED
    assigned_agent_id: Optional[str] = None
    pickup_completed: bool = False
    # Business rule fields
    tags: List[str] = field(default_factory=list)  # ["cash_order", "fragile", "priority"]
    payment_method: str = "card"  # "cash", "card", "prepaid"
    delivery_fee: float = 0.0
    tips: float = 0.0
    max_distance_km: Optional[float] = None  # Max distance for assignment
    declined_by: Set[str] = field(default_factory=set)  # Agent IDs who declined
    # Metadata
    meta: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def restaurant_name(self) -> str:
        return self.meta.get('restaurant_name', 'Unknown Restaurant')
    
    @property
    def customer_name(self) -> str:
        return self.meta.get('customer_name', 'Unknown Customer')
    
    @property
    def is_urgent(self) -> bool:
        """Check if task pickup time is within 15 minutes"""
        now = datetime.now(timezone.utc)
        time_until_pickup = (self.pickup_before - now).total_seconds()
        return time_until_pickup <= 900  # 15 minutes
    
    @property
    def is_overdue(self) -> bool:
        """Check if task pickup time has passed"""
        now = datetime.now(timezone.utc)
        return now > self.pickup_before
    
    def urgency_score(self) -> float:
        """Higher score = more urgent (0-100)"""
        now = datetime.now(timezone.utc)
        time_until_pickup = (self.pickup_before - now).total_seconds()
        
        if time_until_pickup <= 0:
            return 100  # Overdue
        elif time_until_pickup <= 300:  # 5 min
            return 90
        elif time_until_pickup <= 600:  # 10 min
            return 70
        elif time_until_pickup <= 900:  # 15 min
            return 50
        elif time_until_pickup <= 1800:  # 30 min
            return 30
        else:
            return 10
    
    def was_declined_by(self, agent_id: str) -> bool:
        """Check if this task was declined by a specific agent"""
        return agent_id in self.declined_by
    
    def add_decline(self, agent_id: str):
        """Record that an agent declined this task"""
        self.declined_by.add(agent_id)
        self.last_updated = datetime.now(timezone.utc)
    
    @property
    def is_cash_order(self) -> bool:
        """Check if this is a cash order"""
        return self.payment_method == "cash" or "cash_order" in self.tags
    
    @property
    def is_premium_task(self) -> bool:
        """Check if this is a premium task using configured thresholds."""
        return (
            self.tips >= type(self).PREMIUM_TIP_THRESHOLD
            or self.delivery_fee >= type(self).PREMIUM_DELIVERY_FEE_THRESHOLD
        )
    
    @property
    def total_earnings(self) -> float:
        """Total earnings for this task (delivery_fee + tips)"""
        return self.delivery_fee + self.tips


@dataclass
class CurrentTask:
    """Represents a task currently assigned to an agent"""
    id: str
    job_type: str
    restaurant_location: Location
    delivery_location: Location
    pickup_before: datetime
    delivery_before: datetime
    pickup_completed: bool = False
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentState:
    id: str
    name: str
    current_location: Location
    status: AgentStatus = AgentStatus.IDLE
    current_tasks: List[CurrentTask] = field(default_factory=list)
    max_capacity: int = 2  # Max tasks an agent can handle
    # Business rule fields
    tags: List[str] = field(default_factory=list)  # ["cash_enabled", "bike", "zone_a"]
    wallet_balance: float = 0.0  # Cash on hand for cash orders
    priority: Optional[int] = None  # Priority level: 1 = highest priority (premium tasks only)
    blocked_merchants: List[str] = field(default_factory=list)
    blocked_customers: List[str] = field(default_factory=list)
    # Timestamps
    last_location_update: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def is_online(self) -> bool:
        return self.status != AgentStatus.OFFLINE
    
    @property
    def is_idle(self) -> bool:
        return self.is_online and len(self.current_tasks) == 0
    
    @property
    def has_capacity(self) -> bool:
        return self.is_online and len(self.current_tasks) < self.max_capacity
    
    @property
    def available_capacity(self) -> int:
        return max(0, self.max_capacity - len(self.current_tasks))
    
    @property
    def projected_location(self) -> Location:
        """
        Where will this agent be after completing current tasks?
        - If no tasks: current location
        - If has tasks: last delivery location
        """
        if not self.current_tasks:
            return self.current_location
        
        # Find the last task in the chain
        last_task = self.current_tasks[-1]
        return last_task.delivery_location
    
    @property
    def next_destination(self) -> Optional[Location]:
        """
        Where is the agent heading next?
        - If no tasks: None
        - If has task with pickup not done: restaurant location
        - If pickup done: delivery location
        """
        if not self.current_tasks:
            return None
        
        first_task = self.current_tasks[0]
        if first_task.pickup_completed:
            return first_task.delivery_location
        else:
            return first_task.restaurant_location
    
    def has_tag(self, tag: str) -> bool:
        """Check if agent has a specific tag"""
        return tag in self.tags
    
    def is_cash_enabled(self) -> bool:
        """Check if agent can handle cash orders (case-insensitive)
        
        All agents can handle cash BY DEFAULT.
        Only agents with NoCash/no_cash/AbsolutelyNoCash etc. tag are blocked.
        Checks if 'nocash' appears ANYWHERE in the tag (after normalization).
        """
        for tag in self.tags:
            # Normalize: lowercase, remove dashes/spaces/underscores
            normalized = tag.lower().replace('-', '').replace('_', '').replace(' ', '')
            # Check if 'nocash' appears anywhere in the normalized tag
            if 'nocash' in normalized:
                return False  # Agent cannot handle cash
        return True  # Can handle cash (default)
    
    def estimated_time_to_idle(self) -> Optional[float]:
        """
        Estimate seconds until agent is idle (finishes all tasks).
        Returns None if can't estimate.
        """
        if not self.current_tasks:
            return 0
        
        # Simple estimate: sum of delivery deadlines minus now
        # This is a rough approximation
        now = datetime.now(timezone.utc)
        last_delivery = self.current_tasks[-1].delivery_before
        return max(0, (last_delivery - now).total_seconds())


class ProximityTrigger:
    """Represents a condition where an agent is close enough to a task for assignment"""
    def __init__(
        self,
        agent: AgentState,
        task: TaskState,
        distance_km: float,
        trigger_type: str,  # "current_location", "projected_location"
        eligibility_reason: Optional[str] = None
    ):
        self.agent = agent
        self.task = task
        self.distance_km = distance_km
        self.trigger_type = trigger_type
        self.eligibility_reason = eligibility_reason
        self.is_eligible = eligibility_reason is None
        self.timestamp = datetime.now(timezone.utc)
    
    def __repr__(self):
        status = "✓ ELIGIBLE" if self.is_eligible else f"✗ {self.eligibility_reason}"
        return f"ProximityTrigger({self.agent.name} → {self.task.restaurant_name}, {self.distance_km:.2f}km, {status})"


class FleetState:
    """
    The Abstract Map - maintains real-time state of the entire fleet.
    Thread-safe for concurrent WebSocket updates.
    """
    
    def __init__(
        self,
        assignment_radius_km: float = 3.0,
        chain_lookahead_radius_km: float = 5.0,
        max_distance_km: float = 3.0,
        optimization_cooldown_seconds: float = 30.0,
        max_lateness_minutes: int = 45,
        max_pickup_delay_minutes: int = 60,
        road_distance_provider: Optional[
            Callable[[List[Location], List[Location]], Tuple[List[List[float]], str]]
        ] = None
    ):
        """
        Initialize the fleet state.
        
        Args:
            assignment_radius_km: Distance threshold for triggering assignment optimization
            chain_lookahead_radius_km: Distance to look for chain opportunities
            max_distance_km: Maximum distance for agent-task compatibility
            optimization_cooldown_seconds: Minimum time between optimizations per agent
            max_lateness_minutes: Maximum allowed delivery lateness before rejecting assignments
            max_pickup_delay_minutes: Maximum delay after food is ready before rejecting pickup
        """
        self.assignment_radius_km = assignment_radius_km
        self.chain_lookahead_radius_km = chain_lookahead_radius_km
        self.max_distance_km = max_distance_km
        self.optimization_cooldown_seconds = optimization_cooldown_seconds
        self.max_lateness_minutes = max_lateness_minutes
        self.max_pickup_delay_minutes = max_pickup_delay_minutes
        self.wallet_threshold = 500.0  # Minimum wallet balance for cash orders
        self.premium_tip_threshold = TaskState.PREMIUM_TIP_THRESHOLD
        self.premium_delivery_fee_threshold = TaskState.PREMIUM_DELIVERY_FEE_THRESHOLD
        self.default_max_capacity = 2  # Default max tasks per agent
        self.direction_coherence_mode = os.environ.get('DIRECTION_COHERENCE_MODE', 'solver').lower()  # "prefilter" = hard block, "solver" = OR-Tools penalty
        self.merchant_agent_exclusions: Dict[str, List[str]] = {}
        self.customer_agent_exclusions: Dict[str, List[str]] = {}
        self._road_distance_provider = road_distance_provider
        self._road_distance_cache: Dict[
            Tuple[float, float, float, float],
            Tuple[float, float, str]
        ] = {}
        self._road_distance_cache_ttl_seconds = float(
            os.environ.get('ROAD_DISTANCE_CACHE_TTL_SECONDS', '30')
        )
        self._road_distance_fallback_cache_ttl_seconds = float(
            os.environ.get('ROAD_DISTANCE_FALLBACK_CACHE_TTL_SECONDS', '5')
        )
        self._road_distance_timeout_seconds = float(
            os.environ.get('ROAD_DISTANCE_TIMEOUT_SECONDS', '5')
        )
        
        # State storage
        self._agents: Dict[str, AgentState] = {}
        self._tasks: Dict[str, TaskState] = {}  # All tasks (unassigned, in-progress)
        self._geofences: Dict[str, Any] = {}  # Geofence regions
        self._preserved_declines: Dict[str, set] = {}  # Preserved declined_by data across syncs
        self._preserved_tags: Dict[str, list] = {}  # Preserved tags data across syncs (esp. TEST tags)
        self._preserved_assignments: Dict[str, str] = {}  # Preserved optimistic assignments (task_id -> agent_id)
        self._dashboard_unassigned_tasks: set = set()  # Task IDs dashboard explicitly says are unassigned
        self._task_expanded_radii: Dict[str, float] = {}  # Task-specific expanded radii
        
        # Optimization tracking
        self._last_optimization_time: Dict[str, float] = {}  # agent_id -> timestamp
        self._pending_triggers: List[ProximityTrigger] = []
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Statistics
        self._stats = {
            'location_updates': 0,
            'task_updates': 0,
            'proximity_checks': 0,
            'triggers_generated': 0,
            'optimizations_triggered': 0,
            'optimizations_skipped_cooldown': 0
        }
        
        logger.info(f"[FleetState] Initialized with radius={assignment_radius_km}km, "
                   f"max_distance={max_distance_km}km, max_lateness={max_lateness_minutes}min, "
                   f"max_pickup_delay={max_pickup_delay_minutes}min, cooldown={optimization_cooldown_seconds}s")
    
    # =========================================================================
    # AGENT STATE MANAGEMENT
    # =========================================================================
    
    def update_agent_location(
        self,
        agent_id: str,
        lat: float,
        lng: float,
        name: Optional[str] = None
    ) -> List[ProximityTrigger]:
        """
        Update agent's current location and check for proximity triggers.
        
        Returns list of triggers if agent is now within assignment radius of any task.
        """
        with self._lock:
            self._stats['location_updates'] += 1
            
            location = Location(lat=lat, lng=lng)
            now = datetime.now(timezone.utc)
            
            if agent_id in self._agents:
                agent = self._agents[agent_id]
                agent.current_location = location
                agent.last_location_update = now
                agent.last_updated = now
                if name:
                    agent.name = name
            else:
                # Agent not in sync - ignore location update (don't create ghost agents)
                logger.debug(f"[FleetState] Ignoring location update for unknown agent: {name} ({agent_id})")
                return []  # No triggers for unknown agents
            
            # Check proximity triggers
            triggers = self._check_proximity_triggers(agent)
            
            if triggers:
                logger.info(f"[FleetState] {len(triggers)} proximity triggers for {agent.name}")
                for t in triggers:
                    logger.debug(f"  → {t}")
            
            return triggers
    
    def set_agent_online(
        self,
        agent_id: str,
        name: Optional[str] = None,
        location: Optional[Tuple[float, float]] = None,
        priority: Optional[int] = None,
        max_capacity: Optional[int] = None,
        tags: Optional[List[str]] = None,
        wallet_balance: Optional[float] = None,
        blocked_merchants: Optional[List[str]] = None,
        blocked_customers: Optional[List[str]] = None
    ) -> Optional[AgentState]:
        """
        Mark agent as online with full profile data (same format as sync).
        
        Args:
            agent_id: Unique agent identifier
            name: Agent's display name
            location: (lat, lng) tuple
            priority: Priority level (1 = highest, for premium tasks only)
            max_capacity: Max tasks agent can handle (default 2)
            tags: List of tags like ["NoCash", "scooter", "Priority1"]
            wallet_balance: Current cash on hand
        """
        with self._lock:
            if agent_id in self._agents:
                # Update existing agent
                agent = self._agents[agent_id]
                agent.status = AgentStatus.IDLE
                if name:
                    agent.name = name
                if location:
                    agent.current_location = Location(lat=location[0], lng=location[1])
                # Priority: agent:online is SOURCE OF TRUTH - always update (even to None)
                # This allows dashboard to explicitly remove priority by sending undefined/null
                    agent.priority = priority
                if max_capacity is not None:
                    agent.max_capacity = max_capacity
                if tags is not None:
                    agent.tags = tags
                if wallet_balance is not None:
                    agent.wallet_balance = wallet_balance
                if blocked_merchants is not None:
                    agent.blocked_merchants = list(blocked_merchants)
                if blocked_customers is not None:
                    agent.blocked_customers = list(blocked_customers)
                agent.last_updated = datetime.now(timezone.utc)
                priority_str = f" [Priority {agent.priority}]" if agent.priority else ""
                logger.info(f"[FleetState] Agent online: {agent.name}{priority_str}")
                return agent
            elif location:
                # Create new agent with full profile
                agent = AgentState(
                    id=agent_id,
                    name=name or f"Agent {agent_id}",
                    current_location=Location(lat=location[0], lng=location[1]),
                    status=AgentStatus.IDLE,
                    priority=priority,
                    max_capacity=max_capacity or 2,
                    tags=tags or [],
                    wallet_balance=wallet_balance or 0.0,
                    blocked_merchants=list(blocked_merchants or []),
                    blocked_customers=list(blocked_customers or [])
                )
                self._agents[agent_id] = agent
                priority_str = f" [Priority {agent.priority}]" if agent.priority else ""
                logger.info(f"[FleetState] New agent online: {agent.name}{priority_str}")
                return agent
            return None
    
    def set_agent_offline(self, agent_id: str) -> Optional[AgentState]:
        """Mark agent as offline"""
        with self._lock:
            if agent_id in self._agents:
                agent = self._agents[agent_id]
                agent.status = AgentStatus.OFFLINE
                agent.last_updated = datetime.now(timezone.utc)
                logger.info(f"[FleetState] Agent offline: {agent.name}")
                return agent
            return None
    
    def update_agent(
        self,
        agent_id: str,
        name: Optional[str] = None,
        max_capacity: Optional[int] = None,
        tags: Optional[List[str]] = None,
        priority: Optional[int] = None,
        wallet_balance: Optional[float] = None,
        blocked_merchants: Optional[List[str]] = None,
        blocked_customers: Optional[List[str]] = None,
        priority_explicitly_set: bool = False
    ) -> Optional[AgentState]:
        """
        Update agent profile settings (not location).
        Only updates fields that are provided (not None), except priority.
        
        Args:
            priority_explicitly_set: If True, update priority even if None (source of truth behavior)
        """
        with self._lock:
            if agent_id not in self._agents:
                return None
            
            agent = self._agents[agent_id]
            
            if name is not None:
                agent.name = name
            if max_capacity is not None:
                agent.max_capacity = max_capacity
            if tags is not None:
                agent.tags = tags
            # Priority: agent:update is SOURCE OF TRUTH when explicitly_set
            # This allows dashboard to explicitly remove priority by sending undefined/null
            if priority_explicitly_set:
                agent.priority = priority
            elif priority is not None:
                agent.priority = priority
            if wallet_balance is not None:
                agent.wallet_balance = wallet_balance
            if blocked_merchants is not None:
                agent.blocked_merchants = list(blocked_merchants)
            if blocked_customers is not None:
                agent.blocked_customers = list(blocked_customers)
            
            agent.last_updated = datetime.now(timezone.utc)
            return agent

    @staticmethod
    def _normalize_merchant_name(merchant_name: Any) -> str:
        """Normalize dashboard merchant names for policy matching."""
        return str(merchant_name or '').strip().casefold()

    def set_merchant_agent_exclusions(self, exclusions: Dict[str, List[Any]]):
        """Replace the canonical merchant-to-agent exclusion map."""
        normalized: Dict[str, List[str]] = {}
        if isinstance(exclusions, dict):
            for merchant, agent_ids in exclusions.items():
                merchant_key = self._normalize_merchant_name(merchant)
                if not merchant_key or not isinstance(agent_ids, (list, tuple, set)):
                    continue
                normalized[merchant_key] = list(dict.fromkeys(
                    str(agent_id).strip()
                    for agent_id in agent_ids
                    if agent_id is not None and str(agent_id).strip()
                ))

        with self._lock:
            self.merchant_agent_exclusions = normalized
        logger.info(
            f"[FleetState] Replaced merchant-agent exclusions with "
            f"{len(normalized)} merchant(s)"
        )

    def _merchant_exemption_source(
        self,
        agent: AgentState,
        task: TaskState
    ) -> Optional[str]:
        """Return A, C, or A+C when a merchant filter blocks this pairing."""
        merchant_key = self._normalize_merchant_name(task.restaurant_name)
        if not merchant_key:
            return None

        blocked_by_agent = any(
            blocked_merchant
            and blocked_merchant in merchant_key
            for blocked_merchant in (
                self._normalize_merchant_name(name)
                for name in agent.blocked_merchants
            )
        )
        blocked_by_config = any(
            merchant_filter in merchant_key
            and str(agent.id) in blocked_agent_ids
            for merchant_filter, blocked_agent_ids
            in self.merchant_agent_exclusions.items()
        )

        if blocked_by_agent and blocked_by_config:
            return "A+C"
        if blocked_by_agent:
            return "A"
        if blocked_by_config:
            return "C"
        return None

    @staticmethod
    def _exact_customer_username(customer_name: Any) -> str:
        """Preserve Tookan usernames exactly: case, spaces, special characters."""
        if customer_name is None:
            return ''
        return str(customer_name)

    def set_customer_agent_exclusions(self, exclusions: Dict[str, List[Any]]):
        """Replace the canonical customer-username-to-agent exclusion map."""
        normalized: Dict[str, List[str]] = {}
        if isinstance(exclusions, dict):
            for customer, agent_ids in exclusions.items():
                customer_key = self._exact_customer_username(customer)
                if customer_key == '' or not isinstance(agent_ids, (list, tuple, set)):
                    continue
                ids = list(dict.fromkeys(
                    str(agent_id).strip()
                    for agent_id in agent_ids
                    if agent_id is not None and str(agent_id).strip()
                ))
                if customer_key in normalized:
                    normalized[customer_key] = list(dict.fromkeys(
                        normalized[customer_key] + ids
                    ))
                else:
                    normalized[customer_key] = ids

        with self._lock:
            self.customer_agent_exclusions = normalized
        logger.info(
            f"[FleetState] Replaced customer-agent exclusions with "
            f"{len(normalized)} customer(s)"
        )

    def _customer_exemption_source(
        self,
        agent: AgentState,
        task: TaskState
    ) -> Optional[str]:
        """Return A, C, or A+C when an exact customer username blocks this pairing."""
        customer_key = self._exact_customer_username(
            task.meta.get('customer_name') if task.meta else None
        )
        if customer_key == '':
            return None

        blocked_by_agent = any(
            self._exact_customer_username(name) == customer_key
            for name in agent.blocked_customers
        )
        blocked_by_config = (
            customer_key in self.customer_agent_exclusions
            and str(agent.id) in self.customer_agent_exclusions[customer_key]
        )

        if blocked_by_agent and blocked_by_config:
            return "A+C"
        if blocked_by_agent:
            return "A"
        if blocked_by_config:
            return "C"
        return None

    @staticmethod
    def _road_distance_cache_key(
        origin: Location,
        destination: Location
    ) -> Tuple[float, float, float, float]:
        """Round coordinates to about 11m so location updates can reuse results."""
        return (
            round(float(origin.lat), 4),
            round(float(origin.lng), 4),
            round(float(destination.lat), 4),
            round(float(destination.lng), 4)
        )

    def _get_assignment_road_distance(
        self,
        agent: AgentState,
        task: TaskState
    ) -> Tuple[float, str]:
        """
        Return the shortest road distance from the agent's current/projected
        location to pickup. OSRM is primary; Haversine is the failure fallback.
        """
        origins = [agent.current_location]
        if agent.current_tasks:
            projected = agent.projected_location
            if self._road_distance_cache_key(
                projected, task.restaurant_location
            ) != self._road_distance_cache_key(
                agent.current_location, task.restaurant_location
            ):
                origins.append(projected)

        destination = task.restaurant_location
        now = time.time()
        distances: List[float] = []
        sources: List[str] = []
        missing_origins: List[Location] = []

        for origin in origins:
            key = self._road_distance_cache_key(origin, destination)
            cached = self._road_distance_cache.get(key)
            if cached and cached[1] > now:
                distances.append(cached[0])
                sources.append(cached[2])
            else:
                missing_origins.append(origin)

        if missing_origins:
            try:
                if self._road_distance_provider:
                    matrix, source = self._road_distance_provider(
                        missing_origins, [destination]
                    )
                else:
                    from fleet_optimizer import (
                        Location as OptimizerLocation,
                        get_osrm_distances_matrix_with_source
                    )
                    optimizer_origins = [
                        OptimizerLocation(origin.lat, origin.lng)
                        for origin in missing_origins
                    ]
                    optimizer_destination = OptimizerLocation(
                        destination.lat, destination.lng
                    )
                    matrix, source = get_osrm_distances_matrix_with_source(
                        optimizer_origins,
                        [optimizer_destination],
                        timeout_seconds=self._road_distance_timeout_seconds,
                        use_retries=False
                    )

                if len(matrix) != len(missing_origins) or any(
                    len(row) != 1 or row[0] is None for row in matrix
                ):
                    raise ValueError(
                        "Road-distance provider returned an invalid matrix"
                    )
            except Exception as exc:
                source = "haversine_fallback"
                matrix = [[origin.distance_to(destination)] for origin in missing_origins]
                logger.warning(
                    f"[Distance] OSRM road-distance lookup failed for "
                    f"agent={agent.id} task={task.id}; using Haversine: {exc}"
                )

            cache_ttl = (
                self._road_distance_cache_ttl_seconds
                if source == "osrm"
                else self._road_distance_fallback_cache_ttl_seconds
            )
            for index, origin in enumerate(missing_origins):
                distance = float(matrix[index][0])
                key = self._road_distance_cache_key(origin, destination)
                self._road_distance_cache[key] = (
                    distance, now + cache_ttl, source
                )
                distances.append(distance)
                sources.append(source)

            if source != "osrm":
                logger.warning(
                    f"[Distance] Max-distance check used {source}: "
                    f"agent={agent.id} task={task.id}"
                )

        # Opportunistically discard expired entries to bound cache growth.
        if len(self._road_distance_cache) > 1000:
            self._road_distance_cache = {
                key: value
                for key, value in self._road_distance_cache.items()
                if value[1] > now
            }

        distance = min(distances)
        source = (
            "osrm" if sources and all(item == "osrm" for item in sources)
            else "haversine_fallback"
        )
        return distance, source
    
    def update_agent_tasks(
        self,
        agent_id: str,
        tasks: List[Dict[str, Any]]
    ) -> Optional[AgentState]:
        """Update agent's current task list"""
        with self._lock:
            if agent_id not in self._agents:
                return None
            
            agent = self._agents[agent_id]
            agent.current_tasks = []
            
            for t in tasks:
                # Parse locations with validation
                restaurant_loc = t.get('restaurant_location') or [0, 0]
                delivery_loc = t.get('delivery_location') or [0, 0]
                
                # Validate location values
                if not isinstance(restaurant_loc, (list, tuple)) or len(restaurant_loc) < 2:
                    restaurant_loc = [0, 0]
                if restaurant_loc[0] is None or restaurant_loc[1] is None:
                    restaurant_loc = [0, 0]
                
                if not isinstance(delivery_loc, (list, tuple)) or len(delivery_loc) < 2:
                    delivery_loc = [0, 0]
                if delivery_loc[0] is None or delivery_loc[1] is None:
                    delivery_loc = [0, 0]
                
                # Parse times
                pickup_before = t.get('pickup_before')
                delivery_before = t.get('delivery_before')
                
                if isinstance(pickup_before, str):
                    pickup_before = _parse_datetime(pickup_before)
                if isinstance(delivery_before, str):
                    delivery_before = _parse_datetime(delivery_before)
                
                task_meta = dict(t.get('_meta', {}))
                if 'restaurant_name' not in task_meta and t.get('restaurant_name'):
                    task_meta['restaurant_name'] = t['restaurant_name']
                if 'customer_name' not in task_meta and t.get('customer_name'):
                    task_meta['customer_name'] = t['customer_name']

                current_task = CurrentTask(
                    id=t.get('id', ''),
                    job_type=t.get('job_type', 'PAIRED'),
                    restaurant_location=Location(lat=restaurant_loc[0], lng=restaurant_loc[1]),
                    delivery_location=Location(lat=delivery_loc[0], lng=delivery_loc[1]),
                    pickup_before=pickup_before or datetime.now(timezone.utc),
                    delivery_before=delivery_before or datetime.now(timezone.utc),
                    pickup_completed=t.get('pickup_completed', False),
                    meta=task_meta
                )
                agent.current_tasks.append(current_task)
            
            # Update status based on capacity
            if len(agent.current_tasks) >= agent.max_capacity:
                agent.status = AgentStatus.AT_CAPACITY
            elif len(agent.current_tasks) > 0:
                agent.status = AgentStatus.BUSY
            elif agent.status != AgentStatus.OFFLINE:
                agent.status = AgentStatus.IDLE
            
            agent.last_updated = datetime.now(timezone.utc)
            self._stats['task_updates'] += 1
            
            return agent
    
    def record_task_decline(self, task_id: str, agent_ids: List[str], latest_agent_id: Optional[str] = None):
        """
        Record that agents declined a task.
        This moves the task back to UNASSIGNED status so it can be reassigned.
        
        Args:
            task_id: The task that was declined
            agent_ids: List of all agent IDs who have declined this task
            latest_agent_id: The agent who most recently declined (for logging)
        """
        with self._lock:
            # Convert to strings for consistent lookup
            task_id = str(task_id) if task_id else ''
            agent_ids = [str(aid) for aid in agent_ids] if agent_ids else []
            latest_agent_id = str(latest_agent_id) if latest_agent_id else None
            
            # Log the raw and converted agent IDs - INFO level for persistent logging
            logger.info(f"[FleetState] 📝 Recording decline for task {task_id[:20]}... | agents={agent_ids}, latest={latest_agent_id}")
            
            if task_id in self._tasks:
                task = self._tasks[task_id]
                
                # Add all declined agents
                for agent_id in agent_ids:
                    task.declined_by.add(agent_id)
                
                # Log the updated declined_by set - INFO level for persistent logging
                logger.info(f"[FleetState] 📝 Task {task_id[:20]}... declined_by now has {len(task.declined_by)} agents: {list(task.declined_by)}")
                
                # If the task was assigned to the agent who declined, remove the assignment
                previous_agent_id = task.assigned_agent_id
                if previous_agent_id:
                    # Remove task from agent's current_tasks
                    if previous_agent_id in self._agents:
                        agent = self._agents[previous_agent_id]
                        agent.current_tasks = [t for t in agent.current_tasks if t.id != task_id]
                        
                        # Update agent status based on remaining tasks
                        if len(agent.current_tasks) == 0:
                            agent.status = AgentStatus.IDLE
                        elif len(agent.current_tasks) < agent.max_capacity:
                            agent.status = AgentStatus.BUSY
                        # else keep AT_CAPACITY
                    
                    # Clear assignment and reset status to UNASSIGNED
                    task.assigned_agent_id = None
                    task.status = TaskStatus.UNASSIGNED
                    logger.info(f"[FleetState] Task {task_id[:20]}... moved back to UNASSIGNED after decline")
                
                task.last_updated = datetime.now(timezone.utc)
                
                if latest_agent_id and latest_agent_id in self._agents:
                    agent_name = self._agents[latest_agent_id].name
                    logger.info(f"[FleetState] Task {task_id[:20]}... declined by {agent_name} (total: {len(task.declined_by)} agents)")
                else:
                    logger.info(f"[FleetState] Task {task_id[:20]}... has {len(task.declined_by)} declines")
            else:
                # Defensive cleanup for race: decline can arrive when task:create
                # was missed and the task only exists as placeholder capacity load.
                removed = self._remove_task_from_all_agents(task_id)
                if removed > 0:
                    logger.warning(
                        f"[FleetState] Decline for unknown task {task_id[:20]}... "
                        f"cleaned placeholder from {removed} agent(s)"
                    )
    
    def assign_task(self, task_id: str, agent_id: str, agent_name: Optional[str] = None) -> Optional[TaskState]:
        """
        Assign a task to an agent.
        
        Args:
            task_id: The task to assign
            agent_id: The agent to assign it to
            agent_name: Optional agent name for logging
            
        Returns:
            TaskState if assignment was made, None if task not found or already assigned to same agent
        """
        with self._lock:
            # Convert to strings for consistent lookup
            task_id = str(task_id) if task_id else ''
            agent_id = str(agent_id) if agent_id else ''
            
            if task_id not in self._tasks:
                # Task not in fleet state (missed task:created event)
                # Still link to agent so capacity tracking stays accurate
                if agent_id in self._agents:
                    agent = self._agents[agent_id]
                    if not any(t.id == task_id for t in agent.current_tasks):
                        placeholder = Location(0, 0)
                        now = datetime.now(timezone.utc)
                        current_task = CurrentTask(
                            id=task_id,
                            job_type="PAIRED",
                            restaurant_location=placeholder,
                            delivery_location=placeholder,
                            pickup_before=now + timedelta(hours=1),
                            delivery_before=now + timedelta(hours=2),
                        )
                        agent.current_tasks.append(current_task)
                        if len(agent.current_tasks) >= agent.max_capacity:
                            agent.status = AgentStatus.AT_CAPACITY
                        else:
                            agent.status = AgentStatus.BUSY
                        name = agent_name or agent.name
                        logger.warning(
                            f"[FleetState] Task {task_id[:20]}... not found but linked to {name} "
                            f"for capacity tracking (tasks: {len(agent.current_tasks)})"
                        )
                else:
                    logger.warning(f"[FleetState] Cannot assign - task {task_id[:20]}... not found, agent {agent_id} not in state")
                return None
            
            task = self._tasks[task_id]
            
            # DUPLICATE CHECK: Skip if already assigned to this agent
            if task.assigned_agent_id == agent_id and task.status in [TaskStatus.ASSIGNED, TaskStatus.PICKUP_IN_PROGRESS, TaskStatus.DELIVERY_IN_PROGRESS]:
                # Already assigned to this agent - silently skip
                return None
            
            task.assigned_agent_id = agent_id
            task.status = TaskStatus.ASSIGNED
            task.last_updated = datetime.now(timezone.utc)
            
            # GLOBAL DEDUPLICATION: Remove this task from ALL agents first
            # This prevents the same task from being in multiple agents' lists
            for ag in self._agents.values():
                if any(t.id == task_id for t in ag.current_tasks):
                    ag.current_tasks = [t for t in ag.current_tasks if t.id != task_id]
                    # Recalculate status for agents who lost a task
                    if len(ag.current_tasks) == 0:
                        ag.status = AgentStatus.IDLE
                    elif len(ag.current_tasks) < ag.max_capacity:
                        ag.status = AgentStatus.BUSY
            
            # Add to new agent's current tasks if agent exists
            if agent_id in self._agents:
                agent = self._agents[agent_id]
                current_task = CurrentTask(
                    id=task.id,
                    job_type=task.job_type,
                    restaurant_location=task.restaurant_location,
                    delivery_location=task.delivery_location,
                    pickup_before=task.pickup_before,
                    delivery_before=task.delivery_before,
                    pickup_completed=task.pickup_completed,
                    meta=task.meta
                )
                agent.current_tasks.append(current_task)
                
                # Update agent status
                if len(agent.current_tasks) >= agent.max_capacity:
                    agent.status = AgentStatus.AT_CAPACITY
                else:
                    agent.status = AgentStatus.BUSY
                
                logger.info(f"[FleetState] Task {task.restaurant_name} assigned to {agent.name}")
            else:
                name = agent_name or agent_id
                logger.info(f"[FleetState] Task {task.restaurant_name} assigned to {name} (agent not in state)")
            
            return task
    
    def accept_task(self, task_id: str, agent_id: str) -> Optional[TaskState]:
        """
        Mark task as accepted by agent.
        This also assigns the task if not already assigned (handles case where
        dashboard sends task:accepted without task:assigned first).
        """
        with self._lock:
            # Convert to strings for consistent lookup
            task_id = str(task_id) if task_id else ''
            agent_id = str(agent_id) if agent_id else ''
            
            if task_id in self._tasks:
                task = self._tasks[task_id]
                
                # CRITICAL: If task is still UNASSIGNED, assign it now
                # This handles dashboards that send task:accepted without task:assigned
                if task.status == TaskStatus.UNASSIGNED:
                    task.status = TaskStatus.ASSIGNED
                    task.assigned_agent_id = agent_id
                    
                    # Add to agent's current tasks if not already there
                    if agent_id in self._agents:
                        agent = self._agents[agent_id]
                        if not any(t.id == task_id for t in agent.current_tasks):
                            current_task = CurrentTask(
                                id=task.id,
                                job_type=task.job_type,
                                restaurant_location=task.restaurant_location,
                                delivery_location=task.delivery_location,
                                pickup_before=task.pickup_before,
                                delivery_before=task.delivery_before,
                                pickup_completed=task.pickup_completed,
                                meta=task.meta
                            )
                            agent.current_tasks.append(current_task)
                            # Update agent status
                            if len(agent.current_tasks) >= agent.max_capacity:
                                agent.status = AgentStatus.AT_CAPACITY
                            else:
                                agent.status = AgentStatus.BUSY
                
                task.last_updated = datetime.now(timezone.utc)
                
                if agent_id in self._agents:
                    logger.info(f"[FleetState] Task {task.restaurant_name} accepted by {self._agents[agent_id].name}")
                
                return task

            # Task not in fleet state (missed task:created event)
            # Still link to agent so capacity tracking stays accurate
            if agent_id in self._agents:
                agent = self._agents[agent_id]
                if not any(t.id == task_id for t in agent.current_tasks):
                    placeholder = Location(0, 0)
                    now = datetime.now(timezone.utc)
                    current_task = CurrentTask(
                        id=task_id,
                        job_type="PAIRED",
                        restaurant_location=placeholder,
                        delivery_location=placeholder,
                        pickup_before=now + timedelta(hours=1),
                        delivery_before=now + timedelta(hours=2),
                    )
                    agent.current_tasks.append(current_task)
                    if len(agent.current_tasks) >= agent.max_capacity:
                        agent.status = AgentStatus.AT_CAPACITY
                    else:
                        agent.status = AgentStatus.BUSY
                    logger.warning(
                        f"[FleetState] Task {task_id[:20]}... not found but linked to {agent.name} "
                        f"on accept for capacity tracking (tasks: {len(agent.current_tasks)})"
                    )
            return None
    
    def get_agent(self, agent_id: str) -> Optional[AgentState]:
        """Get agent state by ID"""
        with self._lock:
            agent_id = str(agent_id) if agent_id else ''
            return self._agents.get(agent_id)
    
    def get_all_agents(self) -> List[AgentState]:
        """Get all agents"""
        with self._lock:
            return list(self._agents.values())
    
    def get_online_agents(self) -> List[AgentState]:
        """Get all online agents"""
        with self._lock:
            return [a for a in self._agents.values() if a.is_online]
    
    def get_available_agents(self) -> List[AgentState]:
        """Get agents with capacity for more tasks"""
        with self._lock:
            return [a for a in self._agents.values() if a.has_capacity]
    
    # =========================================================================
    # TASK STATE MANAGEMENT
    # =========================================================================
    
    def add_task(self, task_data: Dict[str, Any]) -> TaskState:
        """Add or update a task"""
        with self._lock:
            # Convert task_id to string for consistent lookup
            task_id = str(task_data.get('id', ''))

            # Build meta: prefer _meta from payload, but fall back to top-level fields.
            # Dashboard fleet:sync in_progress_tasks may send restaurant_name/customer_name
            # at the top level without wrapping them in _meta.
            meta = dict(task_data.get('_meta', {}))
            if 'restaurant_name' not in meta and task_data.get('restaurant_name'):
                meta['restaurant_name'] = task_data['restaurant_name']
            if 'customer_name' not in meta and task_data.get('customer_name'):
                meta['customer_name'] = task_data['customer_name']
            
            # Parse locations with validation
            restaurant_loc = task_data.get('restaurant_location') or [0, 0]
            delivery_loc = task_data.get('delivery_location') or [0, 0]
            
            # Validate location values - ensure they're valid numbers
            if not isinstance(restaurant_loc, (list, tuple)) or len(restaurant_loc) < 2:
                restaurant_loc = [0, 0]
            if restaurant_loc[0] is None or restaurant_loc[1] is None:
                logger.warning(f"[FleetState] Task {task_id} has None in restaurant_location: {restaurant_loc}")
                restaurant_loc = [0, 0]
            
            if not isinstance(delivery_loc, (list, tuple)) or len(delivery_loc) < 2:
                delivery_loc = [0, 0]
            if delivery_loc[0] is None or delivery_loc[1] is None:
                logger.warning(f"[FleetState] Task {task_id} has None in delivery_location: {delivery_loc}")
                delivery_loc = [0, 0]
            
            # Parse times
            pickup_before = task_data.get('pickup_before')
            delivery_before = task_data.get('delivery_before')
            
            if isinstance(pickup_before, str):
                pickup_before = _parse_datetime(pickup_before)
            if isinstance(delivery_before, str):
                delivery_before = _parse_datetime(delivery_before)
            
            # Determine status - convert to string for consistent lookup
            assigned_agent_raw = task_data.get('assigned_driver') or task_data.get('assigned_agent_id') or task_data.get('assigned_agent')
            assigned_agent = str(assigned_agent_raw) if assigned_agent_raw else None
            status = TaskStatus.ASSIGNED if assigned_agent else TaskStatus.UNASSIGNED
            
            # Parse business rule fields (include Task_Category so HEAVY/etc. match)
            tags = _extract_incoming_task_tags(task_data, meta)
            payment_method = task_data.get('payment_method', 'card')
            delivery_fee = float(task_data.get('delivery_fee', 0))
            tips = float(task_data.get('tips', 0))
            max_distance_km = task_data.get('max_distance_km')
            if max_distance_km is not None:
                max_distance_km = float(max_distance_km)
            
            # Parse declined_by if dashboard sends it (convert to strings for consistency)
            # Handle both formats: ["agent_id", ...] or [{"driver_id": "agent_id"}, ...]
            incoming_declined_by = set()
            raw_declined = task_data.get('declined_by', [])
            if raw_declined:
                for d in raw_declined:
                    if isinstance(d, dict):
                        driver_id = d.get('driver_id')
                        if driver_id:
                            incoming_declined_by.add(str(driver_id))
                    else:
                        incoming_declined_by.add(str(d))
            
            # IMPORTANT: Determine if this is an "admin reset" scenario where
            # we should clear declined_by to unblock the task:
            # 
            # If a task is in dashboard's unassigned_tasks array, it means admin
            # considers it available for assignment. If it has declined_by populated,
            # clear it - the admin is explicitly making it available again.
            #
            # This handles the case where admin changes status from "Declined" to 
            # "Unassigned" but the dashboard doesn't clear declined_by in the payload.
            
            dashboard_status = task_data.get('status', '').lower() if task_data.get('status') else None
            
            # Check if task is explicitly in dashboard's unassigned_tasks array
            task_in_unassigned_array = task_id in self._dashboard_unassigned_tasks
            
            # Admin reset if task is in unassigned_tasks AND has declines to clear
            # (if task is in in_progress_tasks, don't touch declined_by)
            admin_reset_to_unassigned = (
                task_in_unassigned_array 
                and not assigned_agent 
                and incoming_declined_by  # Only matters if there are declines
            )
            
            if task_id in self._tasks:
                # Update existing
                task = self._tasks[task_id]
                old_assigned_agent = task.assigned_agent_id
                
                task.restaurant_location = Location(lat=restaurant_loc[0], lng=restaurant_loc[1])
                task.delivery_location = Location(lat=delivery_loc[0], lng=delivery_loc[1])
                task.pickup_before = pickup_before or task.pickup_before
                task.delivery_before = delivery_before or task.delivery_before
                
                # CRITICAL: Don't let stale dashboard data (showing unassigned) override
                # our local assignment. Only update assignment if:
                # 1. Dashboard says it's assigned (takes precedence - dashboard is source of truth)
                # 2. OR we didn't have a local assignment
                if assigned_agent:
                    # Dashboard says assigned - use dashboard's data
                    task.assigned_agent_id = assigned_agent
                    task.status = status
                elif not old_assigned_agent:
                    # We didn't have local assignment - use dashboard's data (unassigned)
                    task.assigned_agent_id = assigned_agent
                    task.status = status
                else:
                    # Dashboard says unassigned but we had local assignment - KEEP local!
                    # This prevents stale sync data from re-assigning tasks
                    logger.info(f"[FleetState] ⚡ Keeping local assignment for {task_id[:20]}... (dashboard stale)")
                
                task.pickup_completed = task_data.get('pickup_completed', False)
                task.tags = tags if tags else task.tags
                task.payment_method = payment_method
                task.delivery_fee = delivery_fee
                task.tips = tips
                task.max_distance_km = max_distance_km if max_distance_km is not None else task.max_distance_km
                task.meta = meta if meta else task.meta
                task.last_updated = datetime.now(timezone.utc)
                
                # Handle declined_by based on whether this is an admin reset
                if admin_reset_to_unassigned and task.declined_by:
                    # Admin explicitly set status to "Unassigned" - clear declined_by to unblock task
                    logger.info(f"[FleetState] 🔓 Admin reset task {task_id[:20]}... to Unassigned - clearing declined_by (was {len(task.declined_by)} agents)")
                    task.declined_by = set()
                elif incoming_declined_by:
                    # MERGE declined_by: Keep existing local declines AND add any from dashboard
                    old_count = len(task.declined_by)
                    task.declined_by.update(incoming_declined_by)
                    if len(task.declined_by) > old_count:
                        logger.info(f"[FleetState] 📥 Merged {len(incoming_declined_by)} dashboard declines into task {task_id[:20]}... (now {len(task.declined_by)} total)")
                
                # ALWAYS remove this task from ALL agents first (global deduplication)
                # This prevents the same task from being in multiple agents' lists
                for agent in self._agents.values():
                    if any(t.id == task_id for t in agent.current_tasks):
                        agent.current_tasks = [t for t in agent.current_tasks if t.id != task_id]
                
                # If task is assigned, ensure it's in new agent's current_tasks
                if assigned_agent and assigned_agent in self._agents:
                    agent = self._agents[assigned_agent]
                    if not any(t.id == task_id for t in agent.current_tasks):
                        current_task = CurrentTask(
                            id=task.id,
                            job_type=task.job_type,
                            restaurant_location=task.restaurant_location,
                            delivery_location=task.delivery_location,
                            pickup_before=task.pickup_before,
                            delivery_before=task.delivery_before,
                            pickup_completed=task.pickup_completed,
                            meta=task.meta
                        )
                        agent.current_tasks.append(current_task)
                        
                        if len(agent.current_tasks) >= agent.max_capacity:
                            agent.status = AgentStatus.AT_CAPACITY
                        elif len(agent.current_tasks) > 0:
                            agent.status = AgentStatus.BUSY
            else:
                # Create new - if admin reset to unassigned, don't include declined_by
                task_declined_by = set() if admin_reset_to_unassigned else incoming_declined_by
                if admin_reset_to_unassigned and incoming_declined_by:
                    logger.info(f"[FleetState] 🔓 New task {task_id[:20]}... with status=Unassigned - ignoring {len(incoming_declined_by)} declines (admin reset)")
                
                task = TaskState(
                    id=task_id,
                    job_type=task_data.get('job_type', 'PAIRED'),
                    restaurant_location=Location(lat=restaurant_loc[0], lng=restaurant_loc[1]),
                    delivery_location=Location(lat=delivery_loc[0], lng=delivery_loc[1]),
                    pickup_before=pickup_before or datetime.now(timezone.utc),
                    delivery_before=delivery_before or datetime.now(timezone.utc),
                    status=status,
                    assigned_agent_id=assigned_agent,
                    pickup_completed=task_data.get('pickup_completed', False),
                    tags=tags,
                    payment_method=payment_method,
                    delivery_fee=delivery_fee,
                    tips=tips,
                    max_distance_km=max_distance_km,
                    meta=meta,
                    declined_by=task_declined_by
                )
                self._tasks[task_id] = task
                # Log premium task info for debugging
                premium_status = "⭐ PREMIUM" if task.is_premium_task else "regular"
                logger.info(f"[FleetState] New task added: {task.restaurant_name} → {task.customer_name} | fee=${task.delivery_fee:.2f}, tips=${task.tips:.2f} [{premium_status}]")
            
            # If task is assigned, add to agent's current_tasks
            # First, remove from ALL agents to prevent duplicates (global deduplication)
            if assigned_agent:
                for ag in self._agents.values():
                    if any(t.id == task_id for t in ag.current_tasks):
                        ag.current_tasks = [t for t in ag.current_tasks if t.id != task_id]
                
                if assigned_agent in self._agents:
                    agent = self._agents[assigned_agent]
                    current_task = CurrentTask(
                        id=task.id,
                        job_type=task.job_type,
                        restaurant_location=task.restaurant_location,
                        delivery_location=task.delivery_location,
                        pickup_before=task.pickup_before,
                        delivery_before=task.delivery_before,
                        pickup_completed=task.pickup_completed,
                        meta=task.meta
                    )
                    agent.current_tasks.append(current_task)
                    
                    # Update agent status
                    if len(agent.current_tasks) >= agent.max_capacity:
                        agent.status = AgentStatus.AT_CAPACITY
                    elif len(agent.current_tasks) > 0:
                        agent.status = AgentStatus.BUSY
                    
                    logger.info(f"[FleetState] Task {task.restaurant_name} linked to {agent.name}")
            
            self._stats['task_updates'] += 1
            return task
    
    def update_task_status(self, task_id: str, status: TaskStatus) -> Optional[TaskState]:
        """Update task status"""
        with self._lock:
            task_id = str(task_id) if task_id else ''
            if task_id in self._tasks:
                task = self._tasks[task_id]
                task.status = status
                task.last_updated = datetime.now(timezone.utc)
                return task
            return None

    def _remove_task_from_all_agents(self, task_id: str) -> int:
        """
        Remove a task ID from every agent's current_tasks list.
        Returns how many agents were cleaned.
        """
        removed_from_agents = 0
        for agent in self._agents.values():
            before = len(agent.current_tasks)
            if before == 0:
                continue

            agent.current_tasks = [t for t in agent.current_tasks if t.id != task_id]
            after = len(agent.current_tasks)
            if after == before:
                continue

            removed_from_agents += 1

            # Keep OFFLINE status untouched, otherwise recalculate by task load.
            if agent.status != AgentStatus.OFFLINE:
                if after == 0:
                    agent.status = AgentStatus.IDLE
                elif after >= agent.max_capacity:
                    agent.status = AgentStatus.AT_CAPACITY
                else:
                    agent.status = AgentStatus.BUSY

        return removed_from_agents
    
    def mark_pickup_complete(self, task_id: str) -> Optional[TaskState]:
        """
        Mark pickup as completed for a PAIRED task.
        Does NOT remove task from agent - they still need to do delivery.
        """
        with self._lock:
            task_id = str(task_id) if task_id else ''
            if task_id in self._tasks:
                task = self._tasks[task_id]
                task.pickup_completed = True
                task.last_updated = datetime.now(timezone.utc)
                
                # Also update in agent's current_tasks
                if task.assigned_agent_id and task.assigned_agent_id in self._agents:
                    agent = self._agents[task.assigned_agent_id]
                    for ct in agent.current_tasks:
                        if ct.id == task_id:
                            ct.pickup_completed = True
                            break
                
                logger.info(f"[FleetState] Pickup completed: {task_id[:20]}... (delivery pending)")
                return task
            return None
    
    def complete_task(self, task_id: str) -> Optional[TaskState]:
        """Mark task as completed and remove from active tasks (delivery done)"""
        with self._lock:
            task_id = str(task_id) if task_id else ''
            if task_id in self._tasks:
                task = self._tasks[task_id]
                task.status = TaskStatus.COMPLETED
                task.last_updated = datetime.now(timezone.utc)

                # Always remove globally to clean duplicates/placeholders.
                self._remove_task_from_all_agents(task_id)

                logger.info(f"[FleetState] Task completed: {task_id[:20]}...")
                return task

            # Defensive cleanup for race: completion can arrive before task:create.
            # In that case the task may only exist as a placeholder in agent lists.
            removed = self._remove_task_from_all_agents(task_id)
            if removed > 0:
                logger.warning(
                    f"[FleetState] Completion for unknown task {task_id[:20]}... "
                    f"cleaned placeholder from {removed} agent(s)"
                )
            return None
    
    def cancel_task(self, task_id: str) -> Optional[TaskState]:
        """Mark task as cancelled"""
        with self._lock:
            task_id = str(task_id) if task_id else ''
            if task_id in self._tasks:
                task = self._tasks[task_id]
                task.status = TaskStatus.CANCELLED
                task.last_updated = datetime.now(timezone.utc)

                # Always remove globally to clean duplicates/placeholders.
                self._remove_task_from_all_agents(task_id)

                logger.info(f"[FleetState] Task cancelled: {task_id[:20]}...")
                return task

            # Defensive cleanup for race: cancellation can arrive before task:create.
            removed = self._remove_task_from_all_agents(task_id)
            if removed > 0:
                logger.warning(
                    f"[FleetState] Cancellation for unknown task {task_id[:20]}... "
                    f"cleaned placeholder from {removed} agent(s)"
                )
            return None
    
    def get_task(self, task_id: str) -> Optional[TaskState]:
        """Get task by ID"""
        with self._lock:
            task_id = str(task_id) if task_id else ''
            return self._tasks.get(task_id)
    
    def get_unassigned_tasks(self) -> List[TaskState]:
        """Get all unassigned tasks"""
        with self._lock:
            return [t for t in self._tasks.values() if t.status == TaskStatus.UNASSIGNED]
    
    def get_unassigned_task_count(self) -> int:
        """Get count of unassigned tasks (lightweight check)"""
        with self._lock:
            return sum(1 for t in self._tasks.values() if t.status == TaskStatus.UNASSIGNED)
    
    def get_active_tasks(self) -> List[TaskState]:
        """Get all active (not completed/cancelled) tasks"""
        with self._lock:
            return [t for t in self._tasks.values() 
                    if t.status not in (TaskStatus.COMPLETED, TaskStatus.CANCELLED)]
    
    # =========================================================================
    # PROXIMITY AND ELIGIBILITY CHECKING
    # =========================================================================
    
    def set_task_expanded_radius(self, task_id: str, radius_km: float):
        """Set expanded radius for a specific task."""
        self._task_expanded_radii[task_id] = radius_km
        logger.info(f"[FleetState] Set expanded radius for task {task_id[:20]}...: {radius_km}km")
    
    def get_task_radius(self, task_id: str) -> float:
        """Get the effective radius for a task (expanded or default)."""
        return self._task_expanded_radii.get(task_id, self.assignment_radius_km)
    
    def clear_task_expanded_radius(self, task_id: str):
        """Clear expanded radius when task is assigned/completed."""
        self._task_expanded_radii.pop(task_id, None)
    
    def get_task_expanded_radius(self, task_id: str) -> Optional[float]:
        """Get the expanded radius for a task (if set), or None if using default."""
        return self._task_expanded_radii.get(task_id)
    
    def _check_proximity_triggers(self, agent: AgentState) -> List[ProximityTrigger]:
        """
        Check if agent is within assignment radius of any unassigned task.
        Returns list of proximity triggers.
        
        NOTE: Uses task-specific expanded radii if set, otherwise default assignment_radius_km.
        """
        triggers = []
        
        if not agent.has_capacity:
            return triggers
        
        # Skip agents with invalid location data
        if (agent.current_location is None or 
            agent.current_location.lat is None or 
            agent.current_location.lng is None):
            logger.warning(f"[FleetState] Skipping agent {agent.name} - invalid current_location")
            return triggers
        
        unassigned_tasks = self.get_unassigned_tasks()
        self._stats['proximity_checks'] += len(unassigned_tasks)
        
        for task in unassigned_tasks:
            # Skip tasks with invalid location data
            if (task.restaurant_location is None or 
                task.restaurant_location.lat is None or 
                task.restaurant_location.lng is None):
                logger.warning(f"[FleetState] Skipping task {task.id[:20]}... - invalid restaurant_location")
                continue
            
            # Get task-specific radius (expanded or default)
            task_radius = self.get_task_radius(task.id)
            
            # Check current location
            current_distance = agent.current_location.distance_to(task.restaurant_location)
            
            if current_distance <= task_radius:
                eligibility = self._check_eligibility(agent, task)
                trigger = ProximityTrigger(
                    agent=agent,
                    task=task,
                    distance_km=current_distance,
                    trigger_type="current_location",
                    eligibility_reason=eligibility
                )
                triggers.append(trigger)
                self._stats['triggers_generated'] += 1
            
            # Check projected location (for proactive chaining)
            if agent.current_tasks:  # Only if agent is busy
                # Skip if projected location is invalid
                if (agent.projected_location is None or 
                    agent.projected_location.lat is None or 
                    agent.projected_location.lng is None):
                    continue
                projected_distance = agent.projected_location.distance_to(task.restaurant_location)
                
                # Use larger of task_radius or chain_lookahead_radius_km for projected checks
                projected_radius = max(task_radius, self.chain_lookahead_radius_km)
                
                if projected_distance <= projected_radius:
                    eligibility = self._check_eligibility(agent, task)
                    trigger = ProximityTrigger(
                        agent=agent,
                        task=task,
                        distance_km=projected_distance,
                        trigger_type="projected_location",
                        eligibility_reason=eligibility
                    )
                    triggers.append(trigger)
                    self._stats['triggers_generated'] += 1
        
        return triggers
    
    def _check_eligibility(self, agent: AgentState, task: TaskState, override_max_distance_km: float = None) -> Optional[str]:
        """
        Check if agent is eligible to take task.
        Returns None if eligible, or reason string if not.
        
        Args:
            agent: Agent to check
            task: Task to check
            override_max_distance_km: If provided, use this instead of task's max_distance_km
                                      (used when radius is expanded in proximity broadcast)
        
        Business Rules Checked:
        0. Merchant exemptions - agent or config policy blocks this merchant
        0. Customer exemptions - agent or config policy blocks this exact username
        0. Priority - Priority 1 agents only get premium tasks (configurable thresholds)
        0b. Tag Matching - TEST agents only get TEST tasks, tagged tasks go to matching agents
        1. Declined - Agent hasn't declined this task
        2. Capacity - Agent has room for more tasks
        3. Online - Agent is not offline
        4. Cash Enabled - Agent can handle cash if task is cash order
        5. Wallet Balance - Agent has sufficient balance for cash orders
        6. Geofence - Agent is assigned to task's pickup zone
        7. Max Distance - Agent is within allowed distance (bypassed for Priority 1 on premium tasks)
        """
        # 0. MERCHANT-AGENT EXEMPTIONS
        exemption_source = self._merchant_exemption_source(agent, task)
        if exemption_source:
            logger.info(
                f"[MerchantExemption] Skipping agent={agent.id} "
                f"merchant={task.restaurant_name!r} source={exemption_source}"
            )
            return f"merchant_exemption_{exemption_source}"

        # 0. CUSTOMER-AGENT EXEMPTIONS (exact username match)
        customer_exemption_source = self._customer_exemption_source(agent, task)
        if customer_exemption_source:
            logger.info(
                f"[CustomerExemption] Skipping agent={agent.id} "
                f"customer={task.meta.get('customer_name')!r} source={customer_exemption_source}"
            )
            return f"customer_exemption_{customer_exemption_source}"

        # 0a. PRIORITY AGENT RULES
        # Premium tasks stay available to priority 1. A regular task is an
        # option only while that agent has no assigned task. The broadcast
        # layer also limits them to one regular offer at a time.
        if agent.priority == 1 and not task.is_premium_task:
            if agent.current_tasks:
                return "priority1_non_premium"
        
        # 0b. TAG MATCHING RULES
        # Normalize tags for comparison (lowercase, replace - and space with _)
        agent_tags_lower = [t.lower().replace('-', '_').replace(' ', '_') for t in agent.tags]
        task_tags_lower = [t.lower().replace('-', '_').replace(' ', '_') for t in task.tags] if task.tags else []
        
        # Check if agent has TEST tag (INTERNAL is just a regular tag with no special rules)
        agent_is_test = any(t == 'test' for t in agent_tags_lower)
        # Check if task has TEST tag
        task_is_test = any(t == 'test' for t in task_tags_lower)
        
        # Rule: TEST agents can ONLY get TEST tasks
        if agent_is_test and not task_is_test:
            return "test_agent_non_test_task"
        
        # Rule: TEST tasks can ONLY go to TEST agents
        if task_is_test and not agent_is_test:
            return "test_task_non_test_agent"
        
        # Rule: If task has other tags (excluding TEST), agent must have at least one matching tag
        non_test_task_tags = [t for t in task_tags_lower if t != 'test']
        if non_test_task_tags:
            non_test_agent_tags = [t for t in agent_tags_lower if t != 'test']
            if not any(tag in non_test_agent_tags for tag in non_test_task_tags):
                return "tag_mismatch"
        
        # 1. Check if agent declined this task
        if task.was_declined_by(agent.id):
            return "declined"
        
        # 2. Check capacity
        if not agent.has_capacity:
            return "at_capacity"
        
        # 3. Check online
        if not agent.is_online:
            return "offline"
        
        # 4. Check cash handling capability - only NoCash tag blocks cash orders
        if task.is_cash_order and not agent.is_cash_enabled():
            return "no_cash_tag"
        
        # 5. Check wallet balance for cash orders - reject if agent has too much cash
        if task.is_cash_order:
            if agent.wallet_balance > self.wallet_threshold:
                return f"wallet_too_high ({agent.wallet_balance:.2f} > {self.wallet_threshold:.2f})"
        
        # 6. Shared region policy: overlap union, Region-first precedence/fallback,
        # route coverage, and subscribed-agent reverse locks.
        online_agent_ids = [
            agent_id for agent_id, current_agent in self._agents.items()
            if current_agent.is_online
        ]
        region_decision = evaluate_region_eligibility(
            agent.id, task, self._geofences.values(), online_agent_ids
        )
        if not region_decision['eligible']:
            matched_names = ', '.join(
                region['name'] for region in region_decision['matching_regions']
            )
            logger.info(
                f"[Geofence] BLOCK: agent={agent.name} ({agent.id}) "
                f"task={task.id[:20]} reason={region_decision['reason']} "
                f"matched=[{matched_names}]"
            )
            return region_decision['reason']
        skip_distance_for_region = region_decision['distance_bypass']
        
        # 7. Check max distance (using current or projected location)
        # Priority 1 agents BYPASS distance for premium tasks (but NOT direction check!)
        # Region-matched agents also BYPASS distance
        skip_distance_check = (agent.priority == 1 and task.is_premium_task)
        
        if not skip_distance_check and not skip_distance_for_region:
            # Use override if provided (for expanded radius), otherwise task-specific, otherwise global
            if override_max_distance_km is not None:
                max_dist = override_max_distance_km
            elif task.max_distance_km is not None:
                max_dist = task.max_distance_km
            else:
                max_dist = self.max_distance_km

            if max_dist is not None:
                distance, distance_source = self._get_assignment_road_distance(
                    agent, task
                )
                if distance > max_dist:
                    return (
                        f"too_far ({distance:.1f}km road > {max_dist}km, "
                        f"source={distance_source})"
                    )
        
        # 8. Direction Coherence - New task delivery should be in same direction as existing tasks
        # Prevents inefficient zig-zag routes
        # NOTE: This applies to ALL agents including Priority 1 - we don't want zig-zag routes!
        # When mode is "solver", skip the hard block — the OR-Tools solver will handle it via penalty
        if self.direction_coherence_mode == "prefilter" and agent.current_tasks:
            direction_result = self._check_delivery_direction_coherence(agent, task)
            if direction_result:
                return direction_result
        
        return None  # Eligible!
    
    def _check_delivery_direction_coherence(self, agent: 'AgentState', task: 'TaskState') -> Optional[str]:
        """
        Check if the new task's delivery direction (pickup→delivery) is coherent
        with the agent's existing tasks' delivery directions.
        
        Direction is measured from each task's PICKUP to its DELIVERY, not from
        the agent's location.  This correctly answers "are these tasks heading
        in the same general direction on the island?" regardless of where the
        agent currently sits.
        
        Skipped when direction comparison is meaningless:
        - New task is delivery-only (no pickup→delivery vector)
        - New task's pickup is already completed (agent just needs to deliver)
        - Existing task is delivery-only or has pickup completed
        
        Returns None if coherent, or reason string if not.
        """
        if not agent.current_tasks:
            return None
        
        # Skip for delivery-only tasks or tasks whose pickup is done
        if getattr(task, 'job_type', 'PAIRED') == 'DELIVERY':
            return None
        if task.pickup_completed:
            return None
        
        # New task direction: pickup → delivery
        if not task.restaurant_location or not task.delivery_location:
            return None
        new_pickup_lat = task.restaurant_location.lat
        new_pickup_lng = task.restaurant_location.lng
        new_delta_lat = task.delivery_location.lat - new_pickup_lat
        new_delta_lng = task.delivery_location.lng - new_pickup_lng
        
        new_magnitude = (new_delta_lat**2 + new_delta_lng**2) ** 0.5
        if new_magnitude < 0.001:
            return None  # Pickup and delivery are the same spot
        
        for existing_task in agent.current_tasks:
            if not existing_task.delivery_location or not existing_task.restaurant_location:
                continue
            # Skip delivery-only or pickup-completed existing tasks
            if getattr(existing_task, 'job_type', 'PAIRED') == 'DELIVERY':
                continue
            if existing_task.pickup_completed:
                continue
            
            # Existing task direction: pickup → delivery
            ex_delta_lat = existing_task.delivery_location.lat - existing_task.restaurant_location.lat
            ex_delta_lng = existing_task.delivery_location.lng - existing_task.restaurant_location.lng
            
            ex_magnitude = (ex_delta_lat**2 + ex_delta_lng**2) ** 0.5
            if ex_magnitude < 0.001:
                continue
            
            dot_product = (new_delta_lat * ex_delta_lat) + (new_delta_lng * ex_delta_lng)
            cos_angle = dot_product / (new_magnitude * ex_magnitude)
            
            # cos < -0.3  ≈  angle > 107°  →  opposite directions
            if cos_angle < -0.3:
                # Before blocking, check for interlocking stops.  Tasks with
                # opposite direction vectors can still form an efficient route
                # when their stops spatially overlap (e.g. new delivery is at
                # the existing pickup, so the agent passes through it anyway).
                OVERLAP_THRESHOLD_KM = 1.0
                if (task.delivery_location.distance_to(existing_task.restaurant_location) < OVERLAP_THRESHOLD_KM
                        or task.restaurant_location.distance_to(existing_task.delivery_location) < OVERLAP_THRESHOLD_KM
                        or task.delivery_location.distance_to(existing_task.delivery_location) < OVERLAP_THRESHOLD_KM):
                    continue  # Interlocking stops — not a zig-zag
                
                new_dir = self._get_cardinal_direction(new_delta_lat, new_delta_lng)
                existing_dir = self._get_cardinal_direction(ex_delta_lat, ex_delta_lng)
                return f"opposite_direction ({existing_dir}→{new_dir})"
        
        return None
    
    def _get_cardinal_direction(self, delta_lat: float, delta_lng: float) -> str:
        """Get cardinal direction string from lat/lng deltas."""
        import math
        angle = math.atan2(delta_lng, delta_lat) * 180 / math.pi  # Angle from north
        
        if -22.5 <= angle < 22.5:
            return "N"
        elif 22.5 <= angle < 67.5:
            return "NE"
        elif 67.5 <= angle < 112.5:
            return "E"
        elif 112.5 <= angle < 157.5:
            return "SE"
        elif angle >= 157.5 or angle < -157.5:
            return "S"
        elif -157.5 <= angle < -112.5:
            return "SW"
        elif -112.5 <= angle < -67.5:
            return "W"
        else:  # -67.5 <= angle < -22.5
            return "NW"
    
    def _get_task_regions(self, task: 'TaskState') -> List[Dict[str, Any]]:
        """Return every policy-matching region for the task."""
        return matching_regions(task, self._geofences.values())

    def _get_online_region_agents(self, region_id: str) -> List[str]:
        """
        Get list of online agent IDs assigned to the given region.
        
        Cross-references geofence agent_ids with agents currently online.
        Returns empty list if region not found or no agents online.
        """
        gf = self._geofences.get(region_id)
        if not gf:
            return []
        
        region_agent_ids = gf.get('agent_ids', set())
        online_ids = []
        for agent_id in region_agent_ids:
            agent = self._agents.get(str(agent_id))
            if agent and agent.is_online:
                online_ids.append(str(agent_id))
        return online_ids
    
    def _get_agent_regions(self, agent: 'AgentState') -> List[Dict[str, Any]]:
        """Get every policy region this agent is subscribed to."""
        if not self._geofences:
            return []
        
        regions = []
        agent_id_str = str(agent.id)
        for gf in self._geofences.values():
            normalized = normalize_region(gf)
            if agent_id_str in normalized['agent_ids']:
                regions.append(normalized)
        return regions

    def _point_in_polygon(self, lat: float, lng: float, polygon: List[List[float]]) -> bool:
        """
        Ray casting algorithm to check if point is inside polygon.
        Polygon is list of [lat, lng] points.
        """
        n = len(polygon)
        if n < 3:
            return False
        
        inside = False
        j = n - 1
        
        for i in range(n):
            yi, xi = polygon[i][0], polygon[i][1]
            yj, xj = polygon[j][0], polygon[j][1]
            
            if ((yi > lat) != (yj > lat)) and (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi):
                inside = not inside
            
            j = i
        
        return inside
    
    def find_eligible_agents_for_task(self, task_id: str) -> List[Tuple[AgentState, float, Optional[str]]]:
        """
        Find all agents that could potentially take this task.
        Returns list of (agent, distance_km, eligibility_reason) tuples.
        
        Checks multiple locations for busy agents:
        - Current location (where they are now)
        - Current pickup/delivery destination (where they're heading)
        - Projected location (where they'll be after all current tasks)
        """
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return []
            
            results = []
            for agent in self._agents.values():
                if not agent.is_online:
                    continue
                
                # Calculate distance from multiple locations, use the CLOSEST
                distances = []
                
                # 1. Current location (always check)
                distances.append(agent.current_location.distance_to(task.restaurant_location))
                
                if agent.current_tasks:
                    # 2. Next destination (pickup or delivery they're heading to)
                    if agent.next_destination:
                        distances.append(agent.next_destination.distance_to(task.restaurant_location))
                    
                    # 3. Projected location (where they'll be after all tasks)
                    distances.append(agent.projected_location.distance_to(task.restaurant_location))
                
                # Use the minimum distance (best case)
                distance = min(distances)
                
                eligibility = self._check_eligibility(agent, task)
                results.append((agent, distance, eligibility))
            
            # Sort by distance (closest first)
            results.sort(key=lambda x: x[1])
            return results
    
    def find_tasks_near_agent(
        self,
        agent_id: str,
        radius_km: Optional[float] = None,
        use_projected: bool = False
    ) -> List[Tuple[TaskState, float]]:
        """
        Find unassigned tasks near an agent.
        Returns list of (task, distance_km) tuples.
        """
        with self._lock:
            agent = self._agents.get(agent_id)
            if not agent:
                return []
            
            radius = radius_km or self.assignment_radius_km
            location = agent.projected_location if use_projected else agent.current_location
            
            results = []
            for task in self.get_unassigned_tasks():
                distance = location.distance_to(task.restaurant_location)
                if distance <= radius:
                    results.append((task, distance))
            
            # Sort by distance
            results.sort(key=lambda x: x[1])
            return results
    
    # =========================================================================
    # OPTIMIZATION CONTROL
    # =========================================================================
    
    def should_trigger_optimization(self, agent_id: str) -> bool:
        """
        Check if we should trigger optimization for this agent.
        Implements cooldown to prevent over-optimization.
        """
        with self._lock:
            now = time.time()
            last_opt = self._last_optimization_time.get(agent_id, 0)
            
            if now - last_opt < self.optimization_cooldown_seconds:
                self._stats['optimizations_skipped_cooldown'] += 1
                return False
            
            return True
    
    def record_optimization(self, agent_id: str):
        """Record that optimization was triggered for this agent"""
        with self._lock:
            self._last_optimization_time[agent_id] = time.time()
            self._stats['optimizations_triggered'] += 1
    
    def get_optimization_candidates(self) -> List[ProximityTrigger]:
        """
        Get list of triggers that should result in optimization.
        Filters by eligibility and cooldown.
        """
        with self._lock:
            candidates = []
            
            for agent in self.get_available_agents():
                if not self.should_trigger_optimization(agent.id):
                    continue
                
                triggers = self._check_proximity_triggers(agent)
                eligible_triggers = [t for t in triggers if t.is_eligible]
                
                if eligible_triggers:
                    # Take the best trigger (closest task)
                    best = min(eligible_triggers, key=lambda t: t.distance_km)
                    candidates.append(best)
            
            return candidates
    
    # =========================================================================
    # BULK OPERATIONS (for syncing with dashboard)
    # =========================================================================
    
    def sync_agents(self, agents_data: List[Dict[str, Any]]):
        """Sync all agents from dashboard data - replaces existing agents"""
        with self._lock:
            # PRESERVE priority settings before clearing!
            # Priority is set by agent:online event and should not be lost during fleet:sync
            preserved_priorities = {
                agent_id: agent.priority 
                for agent_id, agent in self._agents.items() 
                if agent.priority is not None
            }
            if preserved_priorities:
                logger.info(f"[FleetState] 🔒 Preserving {len(preserved_priorities)} priority settings during sync")
            
            # Clear existing agents (full sync replaces all)
            self._agents.clear()
            
            for agent_data in agents_data:
                agent_id = str(agent_data.get('id', ''))
                location = agent_data.get('location', [0, 0])
                status_str = agent_data.get('status', 'online')
                
                # Parse status (all synced agents are assumed online, status will be recalculated)
                if status_str == 'offline':
                    status = AgentStatus.OFFLINE
                else:
                    # Will be recalculated based on tasks
                    status = AgentStatus.IDLE
                
                # Priority: Use sync data if provided, otherwise restore preserved value
                sync_priority = agent_data.get('priority')
                preserved_priority = preserved_priorities.get(agent_id)
                final_priority = sync_priority if sync_priority is not None else preserved_priority
                
                if agent_id in self._agents:
                    agent = self._agents[agent_id]
                    agent.current_location = Location(lat=location[0], lng=location[1])
                    agent.name = agent_data.get('name', agent.name)
                    agent.status = status
                    agent.tags = agent_data.get('tags', agent.tags)
                    agent.wallet_balance = float(agent_data.get('wallet_balance', agent.wallet_balance))
                    agent.max_capacity = int(agent_data.get('max_capacity', agent.max_capacity))
                    agent.priority = final_priority
                    agent.blocked_merchants = list(agent_data.get('blocked_merchants', []))
                    agent.blocked_customers = list(agent_data.get('blocked_customers', []))
                else:
                    agent = AgentState(
                        id=agent_id,
                        name=agent_data.get('name', f'Agent {agent_id}'),
                        current_location=Location(lat=location[0], lng=location[1]),
                        status=status,
                        tags=agent_data.get('tags', []),
                        wallet_balance=float(agent_data.get('wallet_balance') or 0),
                        max_capacity=int(agent_data.get('max_capacity', 2)),
                        priority=final_priority,
                        blocked_merchants=list(agent_data.get('blocked_merchants', [])),
                        blocked_customers=list(agent_data.get('blocked_customers', []))
                    )
                    self._agents[agent_id] = agent
                
                agent.last_updated = datetime.now(timezone.utc)
            
            # DEBUG: Log priority agents
            priority_agents = [a for a in self._agents.values() if a.priority is not None]
            if priority_agents:
                for pa in priority_agents:
                    logger.info(f"[FleetState] ⭐ Priority {pa.priority} agent: {pa.name}")
            else:
                # Check if first agent has priority field to diagnose
                if agents_data:
                    first_agent = agents_data[0]
                    has_priority_field = 'priority' in first_agent
                    logger.info(f"[FleetState] ⚠️ No Priority agents found. Dashboard sending 'priority' field: {has_priority_field}")
            
            # DEBUG: Log agents with non-default max_capacity (helps diagnose sync issues)
            non_default_capacity = [(a.name, a.max_capacity) for a in self._agents.values() if a.max_capacity != 2]
            if non_default_capacity:
                logger.info(f"[FleetState] 📊 Agents with custom max_capacity: {non_default_capacity}")
            else:
                # Check what dashboard is sending
                sample_capacities = [(d.get('name', d.get('id'))[:15], d.get('max_capacity', 'NOT_SENT')) for d in agents_data[:3]]
                logger.info(f"[FleetState] ⚠️ All agents have default max_capacity=2. Sample from dashboard: {sample_capacities}")
            
            logger.info(f"[FleetState] Synced {len(agents_data)} agents")
    
    def set_dashboard_unassigned_tasks(self, task_ids: set):
        """Mark task IDs as explicitly unassigned by dashboard.
        
        This prevents optimistic assignment restoration for these tasks.
        If dashboard says a task is unassigned, we trust it - the agent
        may have declined, the assignment may have failed, etc.
        """
        self._dashboard_unassigned_tasks = task_ids
        if task_ids:
            logger.info(f"[FleetState] 📋 Dashboard explicitly marked {len(task_ids)} task(s) as unassigned")
    
    def clear_tasks(self):
        """Clear all tasks (call before full sync)
        
        IMPORTANT: This preserves:
        - declined_by history so agents who declined won't get reassigned
        - tags so TEST tasks keep their TEST tag
        - OPTIMISTIC ASSIGNMENTS so locally-assigned tasks aren't re-assigned
          (dashboard may have stale data showing task as unassigned)
        """
        with self._lock:
            # CRITICAL: Preserve state data before clearing
            preserved_declines = {}
            preserved_tags = {}
            preserved_assignments = {}  # NEW: Preserve optimistic assignments
            
            for task_id, task in self._tasks.items():
                if task.declined_by:
                    preserved_declines[task_id] = set(task.declined_by)
                if task.tags:  # Preserve non-empty tags (especially TEST tags!)
                    preserved_tags[task_id] = list(task.tags)
                # Preserve assignments - if we locally marked a task as assigned,
                # don't let stale dashboard data override it
                if task.assigned_agent_id and task.status in [TaskStatus.ASSIGNED, TaskStatus.PICKUP_IN_PROGRESS, TaskStatus.DELIVERY_IN_PROGRESS]:
                    preserved_assignments[task_id] = task.assigned_agent_id
            
            if preserved_declines:
                logger.info(f"[FleetState] 💾 Preserving declined_by data for {len(preserved_declines)} task(s) across sync")
            if preserved_tags:
                logger.info(f"[FleetState] 💾 Preserving tags for {len(preserved_tags)} task(s) across sync")
            if preserved_assignments:
                logger.info(f"[FleetState] 💾 Preserving optimistic assignments for {len(preserved_assignments)} task(s) across sync")
            
            # Store for restoration after sync
            self._preserved_declines = preserved_declines
            self._preserved_tags = preserved_tags
            self._preserved_assignments = preserved_assignments  # NEW
            
            self._tasks.clear()
            # Also clear agent current_tasks
            for agent in self._agents.values():
                agent.current_tasks.clear()
            logger.info("[FleetState] Cleared all tasks")
    
    def sync_tasks(self, tasks_data: List[Dict[str, Any]]):
        """Sync tasks from dashboard data (adds to existing)"""
        with self._lock:
            for task_data in tasks_data:
                self.add_task(task_data)
            
            # CRITICAL: Restore preserved declined_by data after sync
            # This ensures agents who declined tasks won't get them reassigned
            preserved_declines = getattr(self, '_preserved_declines', {})
            restored_decline_count = 0
            for task_id, declined_by in preserved_declines.items():
                if task_id in self._tasks:
                    task = self._tasks[task_id]
                    # Merge preserved declines with any new declines
                    task.declined_by.update(declined_by)
                    restored_decline_count += 1
                    logger.info(f"[FleetState] 🔄 Restored declined_by for task {task_id[:20]}...: {list(task.declined_by)}")
            
            if restored_decline_count > 0:
                logger.info(f"[FleetState] ✅ Restored declined_by data for {restored_decline_count} task(s)")
            
            # CRITICAL: Restore preserved tags after sync (especially TEST tags!)
            # This ensures TEST tasks keep their tags even if dashboard sends empty tags
            preserved_tags = getattr(self, '_preserved_tags', {})
            restored_tags_count = 0
            for task_id, tags in preserved_tags.items():
                if task_id in self._tasks:
                    task = self._tasks[task_id]
                    # If dashboard sent empty tags but we had tags before, restore them
                    if not task.tags and tags:
                        task.tags = tags
                        restored_tags_count += 1
                        logger.info(f"[FleetState] 🔄 Restored tags for task {task_id[:20]}...: {tags}")
                    # If dashboard sent different tags, merge (keep both)
                    elif task.tags and tags:
                        # Merge tags (use set to avoid duplicates, then convert back to list)
                        merged_tags = list(set(task.tags + tags))
                        if set(merged_tags) != set(task.tags):
                            task.tags = merged_tags
                            restored_tags_count += 1
                            logger.info(f"[FleetState] 🔄 Merged tags for task {task_id[:20]}...: {merged_tags}")
            
            if restored_tags_count > 0:
                logger.info(f"[FleetState] ✅ Restored/merged tags for {restored_tags_count} task(s)")
            
            # CRITICAL: Restore preserved optimistic assignments after sync
            # BUT: Do NOT restore for tasks that dashboard explicitly marked as unassigned!
            # If dashboard says a task is unassigned, trust it (agent declined, assignment failed, etc.)
            preserved_assignments = getattr(self, '_preserved_assignments', {})
            dashboard_unassigned = getattr(self, '_dashboard_unassigned_tasks', set())
            restored_assignment_count = 0
            skipped_count = 0
            for task_id, agent_id in preserved_assignments.items():
                # CRITICAL: Skip if dashboard explicitly says this task is unassigned!
                if task_id in dashboard_unassigned:
                    skipped_count += 1
                    logger.info(f"[FleetState] ⏭️ NOT restoring assignment for {task_id[:20]}... - dashboard says UNASSIGNED")
                    continue
                    
                if task_id in self._tasks:
                    task = self._tasks[task_id]
                    # Only restore if dashboard shows task as unassigned but we had it assigned
                    if not task.assigned_agent_id and agent_id:
                        # Verify agent still exists
                        if agent_id in self._agents:
                            task.assigned_agent_id = agent_id
                            task.status = TaskStatus.ASSIGNED
                            # Re-add to agent's current_tasks
                            agent = self._agents[agent_id]
                            if not any(t.id == task_id for t in agent.current_tasks):
                                agent.current_tasks.append(CurrentTask(
                                    id=task_id,
                                    job_type=task.job_type,
                                    restaurant_location=task.restaurant_location,
                                    delivery_location=task.delivery_location,
                                    pickup_before=task.pickup_before,
                                    delivery_before=task.delivery_before,
                                    pickup_completed=task.pickup_completed,
                                    meta=task.meta
                                ))
                            restored_assignment_count += 1
                            logger.info(f"[FleetState] 🔄 Restored optimistic assignment for task {task_id[:20]}... → {agent.name}")
            
            if restored_assignment_count > 0:
                logger.info(f"[FleetState] ✅ Restored optimistic assignments for {restored_assignment_count} task(s) (prevented re-assignment!)")
            if skipped_count > 0:
                logger.info(f"[FleetState] ⚠️ Skipped {skipped_count} optimistic assignment(s) - dashboard explicitly marked them as unassigned")
            
            # Clear the preserved data and dashboard unassigned set
            # CRITICAL: Also clear preserved_assignments to prevent double-restoration
            # when sync_tasks is called multiple times (once for unassigned, once for in-progress)
            self._preserved_declines = {}
            self._preserved_tags = {}
            self._preserved_assignments = {}
            self._dashboard_unassigned_tasks = set()
            
            # Recalculate all agent statuses based on current_tasks
            self._recalculate_agent_statuses()
            
            logger.info(f"[FleetState] Synced {len(tasks_data)} tasks")
    
    def sync_geofences(self, geofences_data: List[Dict[str, Any]]):
        """Sync geofences from dashboard data.
        
        Defensive: an empty list does NOT clear existing geofences.
        Only a non-empty list overwrites, so a missing or empty fleet:sync
        won't wipe geofence state in production.
        """
        with self._lock:
            if not geofences_data:
                logger.info("[FleetState] sync_geofences called with empty list - preserving existing geofences")
                return

            # Validate and normalize the entire payload before replacing live state.
            next_geofences = {}
            for gf_data in geofences_data:
                normalized = normalize_region(gf_data)
                gf_id = normalized['id']
                if gf_id:
                    next_geofences[gf_id] = normalized

            self._geofences = next_geofences
            
            logger.info(f"[FleetState] Synced {len(self._geofences)} geofences")
    
    def _recalculate_agent_statuses(self):
        """Recalculate all agent statuses based on their current tasks"""
        for agent in self._agents.values():
            if agent.status == AgentStatus.OFFLINE:
                continue  # Don't change offline agents
            
            task_count = len(agent.current_tasks)
            if task_count >= agent.max_capacity:
                agent.status = AgentStatus.AT_CAPACITY
            elif task_count > 0:
                agent.status = AgentStatus.BUSY
            else:
                agent.status = AgentStatus.IDLE

    def _normalize_datetime_value(self, value: Any) -> Optional[datetime]:
        """Coerce sync metadata and timestamps into UTC-aware datetimes."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            return _parse_datetime(value)
        return None

    def _is_local_state_newer_than_snapshot(
        self,
        local_updated_at: Optional[datetime],
        snapshot_started_at: Optional[datetime]
    ) -> bool:
        """Return True when local state changed after this snapshot started building."""
        local_dt = self._normalize_datetime_value(local_updated_at)
        snapshot_dt = self._normalize_datetime_value(snapshot_started_at)
        if local_dt is None or snapshot_dt is None:
            return False
        return local_dt > snapshot_dt

    def _is_active_task_state(self, task: TaskState) -> bool:
        """Active tasks remain visible in fleet state and can be preserved across syncs."""
        return task.status not in (TaskStatus.COMPLETED, TaskStatus.CANCELLED)

    def _agent_to_sync_payload(self, agent: AgentState) -> Dict[str, Any]:
        """Serialize local agent state using the same shape as dashboard fleet:sync agents."""
        payload: Dict[str, Any] = {
            'id': agent.id,
            'name': agent.name,
            'location': [agent.current_location.lat, agent.current_location.lng],
            'status': agent.status.value,
            'max_capacity': agent.max_capacity,
            'tags': list(agent.tags),
            'wallet_balance': agent.wallet_balance
        }
        if agent.priority is not None:
            payload['priority'] = agent.priority
        return payload

    def _task_to_sync_payload(self, task: TaskState) -> Dict[str, Any]:
        """Serialize local task state using the same shape as dashboard fleet:sync tasks."""
        payload: Dict[str, Any] = {
            'id': task.id,
            'job_type': task.job_type,
            'restaurant_location': [task.restaurant_location.lat, task.restaurant_location.lng],
            'delivery_location': [task.delivery_location.lat, task.delivery_location.lng],
            'pickup_before': task.pickup_before.isoformat() if task.pickup_before else None,
            'delivery_before': task.delivery_before.isoformat() if task.delivery_before else None,
            'tags': list(task.tags),
            'payment_method': task.payment_method,
            'delivery_fee': task.delivery_fee,
            'tips': task.tips,
            'declined_by': sorted(task.declined_by),
            '_meta': dict(task.meta)
        }

        if task.max_distance_km is not None:
            payload['max_distance_km'] = task.max_distance_km

        if task.assigned_agent_id and self._is_active_task_state(task):
            payload['assigned_agent_id'] = task.assigned_agent_id
            payload['pickup_completed'] = task.pickup_completed

        return payload

    def _agent_payload_differs_from_local(self, agent_data: Dict[str, Any], agent: AgentState) -> bool:
        """Detect whether the incoming sync row would change meaningful agent state."""
        incoming_location = agent_data.get('location') or [0, 0]
        if not isinstance(incoming_location, (list, tuple)) or len(incoming_location) < 2:
            incoming_location = [0, 0]

        incoming_status = str(agent_data.get('status', 'online')).lower()
        incoming_max_capacity = int(agent_data.get('max_capacity', agent.max_capacity))
        incoming_wallet = float(agent_data.get('wallet_balance') or 0)
        incoming_tags = list(agent_data.get('tags', []))
        incoming_priority = agent_data.get('priority')
        incoming_name = agent_data.get('name', agent.name)

        return any([
            incoming_name != agent.name,
            incoming_status != agent.status.value,
            float(incoming_location[0] or 0) != agent.current_location.lat,
            float(incoming_location[1] or 0) != agent.current_location.lng,
            incoming_max_capacity != agent.max_capacity,
            incoming_wallet != agent.wallet_balance,
            incoming_tags != agent.tags,
            incoming_priority != agent.priority
        ])

    def _task_payload_differs_from_local(self, task_data: Dict[str, Any], task: TaskState) -> bool:
        """Detect whether the incoming sync row would regress local task assignment state."""
        incoming_assigned_raw = (
            task_data.get('assigned_driver')
            or task_data.get('assigned_agent_id')
            or task_data.get('assigned_agent')
        )
        incoming_assigned = str(incoming_assigned_raw) if incoming_assigned_raw else None
        incoming_pickup_completed = bool(task_data.get('pickup_completed', False))

        if task.status in (TaskStatus.COMPLETED, TaskStatus.CANCELLED):
            return True

        return (
            incoming_assigned != task.assigned_agent_id
            or incoming_pickup_completed != task.pickup_completed
        )

    def apply_snapshot_sync(
        self,
        agents_data: List[Dict[str, Any]],
        unassigned_tasks: List[Dict[str, Any]],
        in_progress_tasks: List[Dict[str, Any]],
        snapshot_started_at: Optional[Any] = None,
        snapshot_completed_at: Optional[Any] = None,
        snapshot_generation: Optional[Any] = None,
        sync_request_id: Optional[str] = None,
        sync_source: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Apply a fleet snapshot while preserving any newer local realtime state.

        This guards against snapshots that started building before later task/agent
        events arrived in OR-Tools.
        """
        with self._lock:
            snapshot_started_dt = self._normalize_datetime_value(snapshot_started_at)
            snapshot_completed_dt = self._normalize_datetime_value(snapshot_completed_at)

            local_agents_by_id = dict(self._agents)
            local_tasks_by_id = dict(self._tasks)
            incoming_tasks = [dict(task) for task in (unassigned_tasks or []) + (in_progress_tasks or [])]

            summary: Dict[str, Any] = {
                'request_id': sync_request_id,
                'snapshot_generation': snapshot_generation,
                'snapshot_started_at': snapshot_started_dt.isoformat() if snapshot_started_dt else None,
                'snapshot_completed_at': snapshot_completed_dt.isoformat() if snapshot_completed_dt else None,
                'sync_source': sync_source,
                'incoming_agents': len(agents_data or []),
                'incoming_unassigned_tasks': len(unassigned_tasks or []),
                'incoming_in_progress_tasks': len(in_progress_tasks or []),
                'incoming_tasks': len(incoming_tasks),
                'effective_agents': 0,
                'effective_tasks': 0,
                'protected_seen_agents': 0,
                'protected_seen_tasks': 0,
                'skipped_agent_regressions': 0,
                'skipped_task_regressions': 0,
                'preserved_agent_omissions': 0,
                'preserved_task_omissions': 0,
                'removed_agents': 0,
                'removed_tasks': 0,
                'apply_status': 'applied'
            }

            # Track agents that must survive this sync because newer task state depends on them.
            protected_agent_ids_from_tasks: Set[str] = set()
            if snapshot_started_dt is not None:
                for task in local_tasks_by_id.values():
                    if (
                        task.assigned_agent_id
                        and self._is_active_task_state(task)
                        and self._is_local_state_newer_than_snapshot(task.last_updated, snapshot_started_dt)
                    ):
                        protected_agent_ids_from_tasks.add(task.assigned_agent_id)

            effective_agents: List[Dict[str, Any]] = []
            seen_agent_ids: Set[str] = set()

            for raw_agent_data in agents_data or []:
                agent_data = dict(raw_agent_data)
                agent_id = str(agent_data.get('id', ''))
                if not agent_id:
                    continue

                agent_data['id'] = agent_id
                seen_agent_ids.add(agent_id)
                local_agent = local_agents_by_id.get(agent_id)

                if local_agent and self._is_local_state_newer_than_snapshot(local_agent.last_updated, snapshot_started_dt):
                    summary['protected_seen_agents'] += 1
                    if self._agent_payload_differs_from_local(agent_data, local_agent):
                        summary['skipped_agent_regressions'] += 1
                        logger.warning(
                            f"[FleetState] 🛡️ Preserving newer local agent state for {local_agent.name} ({agent_id}) "
                            f"during fleet:sync request={sync_request_id or 'unknown'}"
                        )
                    effective_agents.append(self._agent_to_sync_payload(local_agent))
                    continue

                effective_agents.append(agent_data)

            for agent_id, local_agent in local_agents_by_id.items():
                if agent_id in seen_agent_ids:
                    continue

                preserve_missing_agent = (
                    self._is_local_state_newer_than_snapshot(local_agent.last_updated, snapshot_started_dt)
                    or agent_id in protected_agent_ids_from_tasks
                )

                if preserve_missing_agent:
                    summary['preserved_agent_omissions'] += 1
                    reason = 'newer_local_state'
                    if agent_id in protected_agent_ids_from_tasks and not self._is_local_state_newer_than_snapshot(local_agent.last_updated, snapshot_started_dt):
                        reason = 'newer_task_reference'
                    logger.warning(
                        f"[FleetState] 🛡️ Preserving omitted agent {local_agent.name} ({agent_id}) during fleet:sync "
                        f"request={sync_request_id or 'unknown'} reason={reason}"
                    )
                    effective_agents.append(self._agent_to_sync_payload(local_agent))
                else:
                    summary['removed_agents'] += 1

            effective_tasks: List[Dict[str, Any]] = []
            seen_task_ids: Set[str] = set()

            for raw_task_data in incoming_tasks:
                task_data = dict(raw_task_data)
                task_id = str(task_data.get('id', ''))
                if not task_id:
                    continue

                task_data['id'] = task_id
                seen_task_ids.add(task_id)
                local_task = local_tasks_by_id.get(task_id)

                if local_task and self._is_local_state_newer_than_snapshot(local_task.last_updated, snapshot_started_dt):
                    summary['protected_seen_tasks'] += 1
                    if self._task_payload_differs_from_local(task_data, local_task):
                        summary['skipped_task_regressions'] += 1
                        logger.warning(
                            f"[FleetState] 🛡️ Preserving newer local task state for {task_id[:20]}... "
                            f"during fleet:sync request={sync_request_id or 'unknown'}"
                        )

                    if self._is_active_task_state(local_task):
                        effective_tasks.append(self._task_to_sync_payload(local_task))
                    continue

                effective_tasks.append(task_data)

            for task_id, local_task in local_tasks_by_id.items():
                if task_id in seen_task_ids:
                    continue

                if not self._is_active_task_state(local_task):
                    continue

                if self._is_local_state_newer_than_snapshot(local_task.last_updated, snapshot_started_dt):
                    summary['preserved_task_omissions'] += 1
                    logger.warning(
                        f"[FleetState] 🛡️ Preserving omitted task {task_id[:20]}... during fleet:sync "
                        f"request={sync_request_id or 'unknown'} newer_than_snapshot=true"
                    )
                    effective_tasks.append(self._task_to_sync_payload(local_task))
                else:
                    summary['removed_tasks'] += 1

            effective_unassigned_ids = {
                str(task.get('id'))
                for task in effective_tasks
                if task.get('id')
                and not (
                    task.get('assigned_driver')
                    or task.get('assigned_agent_id')
                    or task.get('assigned_agent')
                )
            }

            summary['effective_agents'] = len(effective_agents)
            summary['effective_tasks'] = len(effective_tasks)

            # Apply the guarded snapshot using the existing sync code paths.
            self.sync_agents(effective_agents)
            self.clear_tasks()
            self.set_dashboard_unassigned_tasks(effective_unassigned_ids)
            self.sync_tasks(effective_tasks)

            stale_guard_count = (
                summary['protected_seen_agents']
                + summary['protected_seen_tasks']
                + summary['preserved_agent_omissions']
                + summary['preserved_task_omissions']
            )
            authoritative_agent_changes = max(0, summary['incoming_agents'] - summary['protected_seen_agents']) + summary['removed_agents']
            authoritative_task_changes = max(0, summary['incoming_tasks'] - summary['protected_seen_tasks']) + summary['removed_tasks']

            if stale_guard_count == 0:
                summary['apply_status'] = 'applied'
            elif authoritative_agent_changes == 0 and authoritative_task_changes == 0:
                summary['apply_status'] = 'rejected_as_stale'
            else:
                summary['apply_status'] = 'partially_applied'

            log_level = logging.INFO if summary['apply_status'] == 'applied' else logging.WARNING
            logger.log(
                log_level,
                "[FleetState] Snapshot sync summary "
                f"request={sync_request_id or 'unknown'} "
                f"generation={snapshot_generation if snapshot_generation is not None else 'unknown'} "
                f"status={summary['apply_status']} "
                f"incoming_agents={summary['incoming_agents']} effective_agents={summary['effective_agents']} "
                f"incoming_tasks={summary['incoming_tasks']} effective_tasks={summary['effective_tasks']} "
                f"skipped_agent_regressions={summary['skipped_agent_regressions']} "
                f"skipped_task_regressions={summary['skipped_task_regressions']} "
                f"preserved_agent_omissions={summary['preserved_agent_omissions']} "
                f"preserved_task_omissions={summary['preserved_task_omissions']} "
                f"removed_agents={summary['removed_agents']} removed_tasks={summary['removed_tasks']}"
            )

            return summary
    
    # =========================================================================
    # STATISTICS AND DEBUGGING
    # =========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Get fleet state statistics"""
        with self._lock:
            return {
                **self._stats,
                'total_agents': len(self._agents),
                'online_agents': len(self.get_online_agents()),
                'available_agents': len(self.get_available_agents()),
                'total_tasks': len(self._tasks),
                'unassigned_tasks': len(self.get_unassigned_tasks()),
                'active_tasks': len(self.get_active_tasks())
            }
    
    def get_state_summary(self) -> Dict[str, Any]:
        """Get a summary of current fleet state"""
        with self._lock:
            agents_summary = []
            for agent in self._agents.values():
                agents_summary.append({
                    'id': agent.id,
                    'name': agent.name,
                    'status': agent.status.value,
                    'location': agent.current_location.to_tuple(),
                    'projected_location': agent.projected_location.to_tuple(),
                    'current_tasks': len(agent.current_tasks),
                    'capacity': f"{len(agent.current_tasks)}/{agent.max_capacity}",
                    'tags': agent.tags,
                    'wallet_balance': agent.wallet_balance
                })
            
            tasks_summary = []
            for task in self.get_unassigned_tasks():
                tasks_summary.append({
                    'id': task.id[:20] + '...',
                    'restaurant': task.restaurant_name,
                    'customer': task.customer_name,
                    'urgency': task.urgency_score(),
                    'is_urgent': task.is_urgent,
                    'is_overdue': task.is_overdue
                })
            
            return {
                'agents': agents_summary,
                'unassigned_tasks': tasks_summary,
                'stats': self.get_stats()
            }
    
    def clear(self):
        """Clear all state (for testing)"""
        with self._lock:
            self._agents.clear()
            self._tasks.clear()
            self._last_optimization_time.clear()
            self.merchant_agent_exclusions = {}
            self.customer_agent_exclusions = {}
            self._road_distance_cache.clear()
            logger.info("[FleetState] State cleared")
    
    # =========================================================================
    # EXPORT FOR OPTIMIZER (same format as HTTP endpoints)
    # =========================================================================
    
    def export_agents_for_optimizer(self) -> Dict[str, Any]:
        """
        Export agents in the same format as /api/test/or-tools/agents endpoint.
        Used by optimize_fleet() function.
        """
        with self._lock:
            # DUPLICATE DETECTION FIRST: Check if any task appears in multiple agents
            all_task_ids = {}
            for agent in self._agents.values():
                tasks_to_remove = []
                for ct in agent.current_tasks:
                    if ct.id in all_task_ids:
                        # Found duplicate - log warning and mark for removal
                        logger.warning(f"[FleetState] DUPLICATE TASK {str(ct.id)[:20]}... found in {agent.name} and {all_task_ids[ct.id]} - removing from {agent.name}")
                        tasks_to_remove.append(ct.id)
                    else:
                        all_task_ids[ct.id] = agent.name
                # Remove duplicates from this agent
                if tasks_to_remove:
                    agent.current_tasks = [t for t in agent.current_tasks if t.id not in tasks_to_remove]
            
            agents_list = []
            for agent in self._agents.values():
                if not agent.is_online:
                    continue
                
                # Format current tasks
                current_tasks = []
                for ct in agent.current_tasks:
                    current_tasks.append({
                        'id': ct.id,
                        'job_type': ct.job_type,
                        'restaurant_location': [ct.restaurant_location.lat, ct.restaurant_location.lng],
                        'delivery_location': [ct.delivery_location.lat, ct.delivery_location.lng],
                        'pickup_before': ct.pickup_before.isoformat() if ct.pickup_before else None,
                        'delivery_before': ct.delivery_before.isoformat() if ct.delivery_before else None,
                        'assigned_driver': agent.id,
                        'pickup_completed': ct.pickup_completed,
                        '_meta': ct.meta
                    })
                
                agents_list.append({
                    'driver_id': agent.id,  # Agent.from_dict expects 'driver_id'
                    'name': agent.name,
                    'current_location': [agent.current_location.lat, agent.current_location.lng],  # Agent.from_dict expects 'current_location'
                    'current_tasks': current_tasks,
                    'wallet_balance': agent.wallet_balance,
                    'blocked_merchants': list(agent.blocked_merchants),
                    'blocked_customers': list(agent.blocked_customers),
                    '_meta': {
                        'max_tasks': agent.max_capacity,
                        'available_capacity': agent.available_capacity,
                        'tags': agent.tags,
                        # Check if 'nocash' appears anywhere in any tag (handles AbsolutelyNoCash, etc.)
                        'has_no_cash_tag': any('nocash' in t.lower().replace('-', '').replace('_', '').replace(' ', '') for t in agent.tags),
                        'priority': agent.priority  # None if not a priority agent
                    }
                })
            
            # Export geofences
            geofence_data = []
            for gf in self._geofences.values():
                geofence_data.append({
                    'id': gf.get('id', ''),
                    'name': gf.get('name', ''),
                    'polygon': gf.get('polygon', []),
                    'agent_ids': list(gf.get('agent_ids', [])),
                    'sharing_mode': gf.get('sharing_mode'),
                    'route_coverage': gf.get('route_coverage')
                })
            
            return {
                'agents': agents_list,
                'online_agent_ids': [agent.id for agent in self._agents.values() if agent.is_online],
                'geofence_data': geofence_data,
                'settings_used': {
                    'walletNoCashThreshold': self.wallet_threshold,
                    'maxDistanceKm': self.max_distance_km,
                    'maxLatenessMinutes': self.max_lateness_minutes,
                    'maxPickupDelayMinutes': self.max_pickup_delay_minutes,
                    'merchantAgentExclusions': {
                        merchant: list(agent_ids)
                        for merchant, agent_ids in self.merchant_agent_exclusions.items()
                    },
                    'customerAgentExclusions': {
                        customer: list(agent_ids)
                        for customer, agent_ids in self.customer_agent_exclusions.items()
                    }
                }
            }
    
    def export_tasks_for_optimizer(self) -> Dict[str, Any]:
        """
        Export unassigned tasks in the same format as /api/test/or-tools/unassigned-tasks endpoint.
        Used by optimize_fleet() function.
        """
        with self._lock:
            tasks_list = []
            for task in self._tasks.values():
                if task.status != TaskStatus.UNASSIGNED:
                    continue
                
                # Get declined_by from task - convert to format optimizer expects
                # Optimizer expects: [{"driver_id": "123"}, ...] not just ["123", ...]
                # CRITICAL: Ensure all IDs are strings for consistent comparison
                declined_by_list = [str(agent_id) for agent_id in task.declined_by] if hasattr(task, 'declined_by') else []
                declined_by = [{'driver_id': agent_id} for agent_id in declined_by_list]
                
                # Log declined_by for tracking - INFO level for persistent logging
                if declined_by_list:
                    logger.info(f"[FleetState] 📤 Exporting task {task.id[:20]}... with {len(declined_by_list)} declines: {declined_by_list}")
                
                # Build _meta with payment_type in format optimizer expects
                task_meta = dict(task.meta) if task.meta else {}
                # Convert payment_method to payment_type in uppercase for optimizer
                payment_type = "CASH" if task.payment_method.lower() == "cash" else "CARD"
                task_meta['payment_type'] = payment_type
                task_meta['tags'] = task.tags
                
                tasks_list.append({
                    'id': task.id,
                    'job_type': task.job_type,
                    'restaurant_location': [task.restaurant_location.lat, task.restaurant_location.lng],
                    'delivery_location': [task.delivery_location.lat, task.delivery_location.lng],
                    'pickup_before': task.pickup_before.isoformat() if task.pickup_before else None,
                    'delivery_before': task.delivery_before.isoformat() if task.delivery_before else None,
                    'tags': task.tags,
                    'payment_method': task.payment_method,
                    'delivery_fee': task.delivery_fee,
                    'tips': task.tips,
                    'max_distance_km': task.max_distance_km,
                    'declined_by': declined_by,
                    '_meta': task_meta
                })
            
            return {
                'tasks': tasks_list
            }


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

# Create a global fleet state instance
fleet_state = FleetState(
    assignment_radius_km=3.0,
    chain_lookahead_radius_km=5.0,
    max_distance_km=3.0,
    optimization_cooldown_seconds=30.0,
    max_lateness_minutes=45,  # Reject assignments that would cause >45min late deliveries
    max_pickup_delay_minutes=60  # Reject if pickup would be >60min after food ready
)
