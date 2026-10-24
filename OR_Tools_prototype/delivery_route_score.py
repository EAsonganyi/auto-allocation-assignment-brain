"""Choose a broadcast route by minimum delivery lateness.

Every legal stop order is timed with the drive between stops. A pickup has to
happen before its own delivery. Arriving before the food is ready means waiting.
Arriving after the food is ready does not count against the agent.

The score is 100 minus the minutes of delivery lateness on the best order.
An order is rejected when the deliveries' lateness adds up to more than the
cutoff. One delivery can be later than another; only the sum decides.
"""

from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, Sequence, Tuple


TravelFn = Callable[[float, float, float, float], int]
MAX_EXACT_STOPS = 14


def choose_delivery_tasks(
    origin_lat: float,
    origin_lng: float,
    committed: Sequence[dict],
    candidates: Sequence[dict],
    now: datetime,
    max_lateness_minutes: int,
    travel_seconds: Optional[TravelFn] = None,
) -> dict:
    """Pick the largest candidate set whose best route stays within lateness.

    Among those sets, the one with the least total delivery lateness wins.
    ``max_lateness_minutes`` is the combined delivery-lateness budget for the
    whole route.
    """
    now = _aware(now)
    max_lateness_seconds = max(0, int(max_lateness_minutes)) * 60
    committed_stops = _stops_for_tasks(committed)
    candidate_groups = [(task['task_id'], _stops_for_tasks([task])) for task in candidates]
    all_stops = list(committed_stops)
    for _, group in candidate_groups:
        all_stops.extend(group)

    if not candidates:
        return _empty_result('no_candidates')

    matrix = _travel_matrix(origin_lat, origin_lng, all_stops, travel_seconds)

    best = None
    group_count = len(candidate_groups)
    for mask in range(1, 1 << group_count):
        selected_tasks = []
        selected_stops = list(committed_stops)
        for index, (task_id, group) in enumerate(candidate_groups):
            if mask & (1 << index):
                selected_tasks.append(task_id)
                selected_stops.extend(group)
        route = _solve(
            selected_stops,
            _slice_matrix(matrix, all_stops, selected_stops),
            now,
            max_lateness_seconds,
        )
        if route is None:
            continue
        rank = (
            len(selected_tasks),
            -route['total_lateness_seconds'],
            -route['max_lateness_seconds'],
        )
        if best is None or rank > best[0]:
            best = (rank, selected_tasks, route)

    if best is None:
        return _empty_result('excessive_delivery_lateness')

    _, selected_tasks, route = best
    minutes = int(round(route['total_lateness_seconds'] / 60.0))
    return {
        'feasible': True,
        'selected_task_ids': selected_tasks,
        'score': max(0, 100 - minutes),
        'delivery_lateness_minutes': minutes,
        'total_lateness_seconds': route['total_lateness_seconds'],
        'max_lateness_seconds': route['max_lateness_seconds'],
        'stops': route['stops'],
        'reason': None,
    }


def _empty_result(reason: str) -> dict:
    return {
        'feasible': False,
        'selected_task_ids': [],
        'score': None,
        'delivery_lateness_minutes': None,
        'total_lateness_seconds': None,
        'max_lateness_seconds': None,
        'stops': [],
        'reason': reason,
    }


def _stops_for_tasks(tasks: Sequence[dict]) -> List[dict]:
    stops = []
    for task in tasks:
        job_type = str(task.get('job_type') or 'PAIRED').upper()
        pickup_done = bool(task.get('pickup_completed'))
        needs_pickup = job_type != 'DELIVERY' and not pickup_done
        needs_delivery = job_type != 'PICKUP'
        if needs_pickup:
            stops.append({
                'task_id': str(task['task_id']),
                'is_new': bool(task.get('is_new')),
                'kind': 'pickup',
                'lat': float(task['pickup_lat']),
                'lng': float(task['pickup_lng']),
                'ready': _aware(task.get('pickup_ready')),
                'deadline': None,
                'label': task.get('label') or '',
            })
        if needs_delivery:
            stops.append({
                'task_id': str(task['task_id']),
                'is_new': bool(task.get('is_new')),
                'kind': 'delivery',
                'lat': float(task['delivery_lat']),
                'lng': float(task['delivery_lng']),
                'ready': None,
                'deadline': _aware(task.get('delivery_deadline')),
                'label': task.get('label') or '',
            })
    return stops


def _solve(
    stops: Sequence[dict],
    matrix: List[List[int]],
    now: datetime,
    max_total_lateness_seconds: Optional[int],
) -> Optional[dict]:
    """Minimum total delivery lateness over legal stop orders.

    ``max_total_lateness_seconds`` is the combined budget for every delivery on
    the route. ``None`` allows any total.
    """
    count = len(stops)
    if count == 0:
        return {
            'total_lateness_seconds': 0,
            'max_lateness_seconds': 0,
            'stops': [],
            'path': (),
        }
    if count > MAX_EXACT_STOPS:
        return _solve_capped(stops, matrix, now, max_total_lateness_seconds)

    pickup_bit = {
        stop['task_id']: index
        for index, stop in enumerate(stops)
        if stop['kind'] == 'pickup'
    }
    ready_seconds = [_seconds_from(now, stop.get('ready')) for stop in stops]
    deadline_seconds = [_seconds_from(now, stop.get('deadline')) for stop in stops]

    # (arrival, total lateness, max lateness, path)
    states = {(0, -1): [(0, 0, 0, ())]}
    for completed in range(count):
        keys = [key for key in states if _bit_count(key[0]) == completed]
        for mask, last in keys:
            for arrival, total, max_late, path in states[(mask, last)]:
                for index in range(count):
                    if mask & (1 << index):
                        continue
                    stop = stops[index]
                    if stop['kind'] == 'delivery':
                        pickup_index = pickup_bit.get(stop['task_id'])
                        if pickup_index is not None and not (mask & (1 << pickup_index)):
                            continue
                    next_arrival = arrival + matrix[last + 1][index + 1]
                    late = 0
                    if stop['kind'] == 'pickup' and ready_seconds[index] is not None:
                        if next_arrival < ready_seconds[index]:
                            next_arrival = ready_seconds[index]
                    elif stop['kind'] == 'delivery' and deadline_seconds[index] is not None:
                        late = max(0, next_arrival - deadline_seconds[index])
                    next_total = total + late
                    if max_total_lateness_seconds is not None and next_total > max_total_lateness_seconds:
                        continue
                    _add_state(
                        states,
                        mask | (1 << index),
                        index,
                        (next_arrival, next_total, max(max_late, late), path + (index,)),
                    )

    full = (1 << count) - 1
    best = None
    for last in range(count):
        for node in states.get((full, last), []):
            rank = (node[1], node[2], node[0])
            if best is None or rank < best[0]:
                best = (rank, node)
    if best is None:
        return None
    _rank, chosen = best
    return _replay(stops, matrix, now, chosen[3], chosen[1], chosen[2])


def _solve_capped(stops, matrix, now, max_total_lateness_seconds) -> Optional[dict]:
    """Insert new stops into the committed order when the route is very long."""
    committed = [index for index, stop in enumerate(stops) if not stop['is_new']]
    fresh = [index for index, stop in enumerate(stops) if stop['is_new']]
    base = _solve(
        [stops[index] for index in committed],
        _slice_matrix(matrix, stops, [stops[index] for index in committed]),
        now,
        None,
    )
    committed_order = list(base['path']) if base else committed
    remapped = []
    committed_stops = [stops[index] for index in committed]
    for local_index in committed_order:
        remapped.append(committed[local_index] if local_index < len(committed) else local_index)

    orders = _interleave(remapped, fresh, stops)
    best = None
    for order in orders:
        replay_stops = [stops[index] for index in order]
        local_matrix = _slice_matrix(matrix, stops, replay_stops)
        route = _replay_if_allowed(replay_stops, local_matrix, now, tuple(range(len(order))), max_total_lateness_seconds)
        if route is None:
            continue
        if best is None or (route['total_lateness_seconds'], route['max_lateness_seconds']) < (
            best['total_lateness_seconds'], best['max_lateness_seconds']
        ):
            best = route
    return best


def _interleave(committed: List[int], fresh: List[int], stops: Sequence[dict]) -> List[Tuple[int, ...]]:
    """All ways to place the new stops around a fixed committed order."""
    if not fresh:
        return [tuple(committed)]
    pickup_of = {}
    for index in fresh:
        if stops[index]['kind'] == 'pickup':
            pickup_of[stops[index]['task_id']] = index
    results = []

    def merge(committed_index: int, pending: Tuple[int, ...], chosen: Tuple[int, ...]):
        if committed_index == len(committed) and not pending:
            results.append(chosen)
            return
        if len(results) > 5000:
            return
        if committed_index < len(committed):
            merge(committed_index + 1, pending, chosen + (committed[committed_index],))
        for position, index in enumerate(pending):
            stop = stops[index]
            if stop['kind'] == 'delivery':
                pickup_index = pickup_of.get(stop['task_id'])
                if pickup_index is not None and pickup_index in pending:
                    continue
            merge(committed_index, pending[:position] + pending[position + 1:], chosen + (index,))

    merge(0, tuple(fresh), ())
    if not results and committed:
        results.append(tuple(committed + fresh))
    return results


def _replay_if_allowed(stops, matrix, now, path, max_total_lateness_seconds) -> Optional[dict]:
    route = _replay(stops, matrix, now, path, 0, 0)
    total = int(route['total_lateness_seconds'])
    if max_total_lateness_seconds is not None and total > max_total_lateness_seconds:
        return None
    return route


def _replay(stops, matrix, now, path, total, max_late) -> dict:
    clock = 0
    last = -1
    built = []
    measured_total = 0
    measured_max = 0
    for index in path:
        stop = stops[index]
        arrival = clock + matrix[last + 1][index + 1]
        wait = 0
        late = 0
        if stop['kind'] == 'pickup' and stop.get('ready') is not None:
            ready = _seconds_from(now, stop['ready'])
            if ready is not None and arrival < ready:
                wait = ready - arrival
                arrival = ready
        elif stop['kind'] == 'delivery' and stop.get('deadline') is not None:
            deadline = _seconds_from(now, stop['deadline'])
            if deadline is not None:
                late = max(0, arrival - deadline)
        measured_total += late
        measured_max = max(measured_max, late)
        kind = stop['kind']
        owner = 'new' if stop['is_new'] else 'existing'
        payload = {
            'type': f'{owner}_task_{kind}',
            'kind': kind,
            'task_id': stop['task_id'],
            'location': [stop['lat'], stop['lng']],
            'arrival_time': (now + timedelta(seconds=arrival)).isoformat(),
            'label': stop.get('label') or '',
        }
        if kind == 'pickup':
            payload['wait_seconds'] = int(wait)
            payload['late_seconds'] = 0
            if stop.get('ready') is not None:
                ready = _seconds_from(now, stop['ready'])
                payload['late_seconds'] = int(max(0, arrival - (ready or 0)))
        else:
            payload['lateness_seconds'] = int(late)
        built.append(payload)
        clock = arrival
        last = index
    return {
        'total_lateness_seconds': measured_total,
        'max_lateness_seconds': measured_max,
        'stops': built,
        'path': path,
    }


def _add_state(states, mask: int, last: int, node: tuple) -> None:
    key = (mask, last)
    bucket = states.get(key, [])
    arrival, total, max_late, _path = node
    kept = []
    for other in bucket:
        other_arrival, other_total, other_max, _other_path = other
        if other_arrival <= arrival and other_total <= total and other_max <= max_late:
            return
        if arrival <= other_arrival and total <= other_total and max_late <= other_max:
            continue
        kept.append(other)
    kept.append(node)
    states[key] = kept


def _travel_matrix(origin_lat, origin_lng, stops, travel_seconds: Optional[TravelFn]) -> List[List[int]]:
    points = [(origin_lat, origin_lng)] + [(stop['lat'], stop['lng']) for stop in stops]
    size = len(points)
    if travel_seconds is None:
        from fleet_optimizer import Location, get_travel_time_matrix
        locations = [Location(lat, lng) for lat, lng in points]
        return get_travel_time_matrix(locations)

    matrix = [[0] * size for _ in range(size)]
    for row in range(size):
        for col in range(size):
            if row == col:
                continue
            matrix[row][col] = max(0, int(travel_seconds(points[row][0], points[row][1], points[col][0], points[col][1])))
    return matrix


def _slice_matrix(matrix, source_stops, chosen_stops) -> List[List[int]]:
    if len(source_stops) == len(chosen_stops) and all(a is b for a, b in zip(source_stops, chosen_stops)):
        return matrix
    source_index = {id(stop): index for index, stop in enumerate(source_stops)}
    chosen_indexes = [source_index[id(stop)] for stop in chosen_stops]
    size = len(chosen_indexes) + 1
    sliced = [[0] * size for _ in range(size)]
    for local_row, source_row in enumerate(chosen_indexes):
        sliced[0][local_row + 1] = matrix[0][source_row + 1]
        sliced[local_row + 1][0] = matrix[source_row + 1][0]
        for local_col, source_col in enumerate(chosen_indexes):
            sliced[local_row + 1][local_col + 1] = matrix[source_row + 1][source_col + 1]
    return sliced


def _seconds_from(now: datetime, moment: Optional[datetime]) -> Optional[int]:
    if moment is None:
        return None
    return int(round((_aware(moment) - now).total_seconds()))


def _aware(moment: Optional[datetime]) -> Optional[datetime]:
    if moment is None:
        return None
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment


def _bit_count(value: int) -> int:
    return bin(value).count('1')
