"""Delivery-lateness broadcast routing."""

from datetime import datetime, timedelta, timezone
import unittest

from delivery_route_score import choose_delivery_tasks


NOW = datetime(2026, 10, 23, 17, 0, tzinfo=timezone.utc)  # 5:00pm UTC


def minutes_later(minutes):
    return NOW + timedelta(minutes=minutes)


def task(task_id, pickup, delivery, ready_min, deadline_min, is_new=True):
    return {
        'task_id': task_id,
        'is_new': is_new,
        'job_type': 'PAIRED',
        'pickup_completed': False,
        'pickup_lat': pickup[0],
        'pickup_lng': pickup[1],
        'delivery_lat': delivery[0],
        'delivery_lng': delivery[1],
        'pickup_ready': minutes_later(ready_min),
        'delivery_deadline': minutes_later(deadline_min),
        'label': task_id,
    }


def table_travel(table):
    def travel(lat1, lng1, lat2, lng2):
        key = (int(lat1), int(lng1), int(lat2), int(lng2))
        return int(table.get(key, 999) * 60)
    return travel


JORDAN = (0, 0)
RA = (1, 0)
RB = (2, 0)
CA = (3, 0)
CB = (4, 0)
SAM = (5, 0)
RILEY = (6, 0)

ROADS = {
    (JORDAN + RA): 5,
    (JORDAN + RB): 10,
    (SAM + RA): 15,
    (SAM + RB): 20,
    (RILEY + RA): 40,
    (RILEY + RB): 40,
    (RA + RB): 5,
    (RB + RA): 5,
    (RA + CA): 20,
    (RA + CB): 30,
    (RB + CA): 20,
    (RB + CB): 25,
    (CA + CB): 10,
    (CB + CA): 10,
    (CA + RA): 20,
    (CA + RB): 25,
    (CB + RA): 35,
    (CB + RB): 30,
}


class DeliveryRouteScoreTest(unittest.TestCase):
    def test_both_pickups_first_beats_finishing_the_current_task(self):
        result = choose_delivery_tasks(
            JORDAN[0], JORDAN[1],
            committed=[task('A', RA, CA, 0, 35, is_new=False)],
            candidates=[task('B', RB, CB, 0, 55, is_new=True)],
            now=NOW,
            max_lateness_minutes=45,
            travel_seconds=table_travel(ROADS),
        )
        self.assertTrue(result['feasible'])
        self.assertEqual(result['selected_task_ids'], ['B'])
        self.assertEqual(result['delivery_lateness_minutes'], 0)
        self.assertEqual(result['score'], 100)
        kinds = [(stop['task_id'], stop['kind']) for stop in result['stops']]
        self.assertEqual(kinds, [
            ('A', 'pickup'),
            ('B', 'pickup'),
            ('A', 'delivery'),
            ('B', 'delivery'),
        ])

    def test_modest_lateness_stays_eligible_with_a_lower_score(self):
        result = choose_delivery_tasks(
            SAM[0], SAM[1],
            committed=[task('A', RA, CA, 0, 35, is_new=False)],
            candidates=[task('B', RB, CB, 0, 55, is_new=True)],
            now=NOW,
            max_lateness_minutes=45,
            travel_seconds=table_travel(ROADS),
        )
        self.assertTrue(result['feasible'])
        self.assertEqual(result['delivery_lateness_minutes'], 5)
        self.assertEqual(result['score'], 95)

    def test_agent_is_left_out_when_every_order_is_excessively_late(self):
        result = choose_delivery_tasks(
            RILEY[0], RILEY[1],
            committed=[task('A', RA, CA, 0, 35, is_new=False)],
            candidates=[task('B', RB, CB, 0, 55, is_new=True)],
            now=NOW,
            max_lateness_minutes=25,
            travel_seconds=table_travel(ROADS),
        )
        self.assertFalse(result['feasible'])
        self.assertEqual(result['reason'], 'excessive_delivery_lateness')
        self.assertIsNone(result['score'])

    def test_late_pickup_does_not_lower_the_score_when_delivery_is_on_time(self):
        origin = (0, 0)
        restaurant = (1, 0)
        customer = (2, 0)
        roads = {
            (origin + restaurant): 30,
            (restaurant + customer): 10,
            (customer + restaurant): 10,
        }
        result = choose_delivery_tasks(
            origin[0], origin[1],
            committed=[],
            candidates=[task('B', restaurant, customer, 0, 50)],
            now=NOW,
            max_lateness_minutes=45,
            travel_seconds=table_travel(roads),
        )
        self.assertTrue(result['feasible'])
        self.assertEqual(result['score'], 100)
        self.assertGreater(result['stops'][0]['late_seconds'], 0)

    def test_waiting_for_food_can_make_the_delivery_late(self):
        origin = (0, 0)
        restaurant = (1, 0)
        customer = (2, 0)
        roads = {
            (origin + restaurant): 5,
            (restaurant + customer): 20,
            (customer + restaurant): 20,
        }
        result = choose_delivery_tasks(
            origin[0], origin[1],
            committed=[],
            candidates=[task('B', restaurant, customer, 30, 40)],
            now=NOW,
            max_lateness_minutes=45,
            travel_seconds=table_travel(roads),
        )
        self.assertTrue(result['feasible'])
        self.assertEqual(result['delivery_lateness_minutes'], 10)
        self.assertEqual(result['score'], 90)

    def test_cutoff_is_the_sum_of_delivery_lateness(self):
        origin = (0, 0)
        restaurant_a = (1, 0)
        customer_a = (2, 0)
        restaurant_b = (3, 0)
        customer_b = (4, 0)
        roads = {
            (origin + restaurant_a): 5,
            (restaurant_a + customer_a): 25,
            (customer_a + restaurant_b): 5,
            (restaurant_b + customer_b): 25,
            (origin + restaurant_b): 40,
            (restaurant_b + restaurant_a): 40,
            (restaurant_a + restaurant_b): 40,
            (customer_b + customer_a): 40,
        }
        shared = dict(
            origin_lat=origin[0],
            origin_lng=origin[1],
            committed=[task('A', restaurant_a, customer_a, 0, 10, is_new=False)],
            candidates=[task('B', restaurant_b, customer_b, 0, 40)],
            now=NOW,
            travel_seconds=table_travel(roads),
        )
        # A is 20 min late and B is 20 min late. Each is under 30, the sum is 40.
        over = choose_delivery_tasks(max_lateness_minutes=30, **shared)
        self.assertFalse(over['feasible'])
        self.assertEqual(over['reason'], 'excessive_delivery_lateness')

        under = choose_delivery_tasks(max_lateness_minutes=45, **shared)
        self.assertTrue(under['feasible'])
        self.assertEqual(under['delivery_lateness_minutes'], 40)
        self.assertEqual(under['score'], 60)

    def test_least_delivery_lateness_wins_over_the_shorter_finish(self):
        origin = (0, 0)
        restaurant_a = (1, 0)
        customer_a = (2, 0)
        restaurant_b = (3, 0)
        customer_b = (4, 0)
        roads = {
            (origin + restaurant_a): 2,
            (restaurant_a + customer_a): 3,
            (customer_a + restaurant_b): 20,
            (restaurant_b + customer_b): 20,
            (origin + restaurant_b): 2,
            (customer_b + restaurant_a): 2,
        }
        result = choose_delivery_tasks(
            origin[0], origin[1],
            committed=[task('A', restaurant_a, customer_a, 0, 6, is_new=False)],
            candidates=[task('B', restaurant_b, customer_b, 0, 50)],
            now=NOW,
            max_lateness_minutes=45,
            travel_seconds=table_travel(roads),
        )
        self.assertTrue(result['feasible'])
        self.assertEqual(result['delivery_lateness_minutes'], 0)
        self.assertEqual(result['score'], 100)
        kinds = [(stop['task_id'], stop['kind']) for stop in result['stops']]
        self.assertEqual(kinds[:2], [('A', 'pickup'), ('A', 'delivery')])

    def test_batch_keeps_the_task_that_fits_and_drops_the_one_that_does_not(self):
        origin = (0, 0)
        restaurant_b = (1, 0)
        customer_b = (2, 0)
        restaurant_c = (3, 0)
        customer_c = (4, 0)
        roads = {
            (origin + restaurant_b): 5,
            (origin + restaurant_c): 5,
            (restaurant_b + customer_b): 10,
            (customer_b + restaurant_b): 10,
            (restaurant_c + customer_c): 80,
            (customer_c + restaurant_c): 80,
            (restaurant_b + restaurant_c): 5,
            (restaurant_c + restaurant_b): 5,
            (customer_b + customer_c): 70,
            (customer_c + customer_b): 70,
            (restaurant_b + customer_c): 80,
            (restaurant_c + customer_b): 80,
            (customer_b + restaurant_c): 70,
            (customer_c + restaurant_b): 70,
        }
        result = choose_delivery_tasks(
            origin[0], origin[1],
            committed=[],
            candidates=[
                task('B', restaurant_b, customer_b, 0, 30),
                task('C', restaurant_c, customer_c, 0, 30),
            ],
            now=NOW,
            max_lateness_minutes=20,
            travel_seconds=table_travel(roads),
        )
        self.assertTrue(result['feasible'])
        self.assertEqual(result['selected_task_ids'], ['B'])
        self.assertEqual(result['score'], 100)


if __name__ == '__main__':
    unittest.main()
