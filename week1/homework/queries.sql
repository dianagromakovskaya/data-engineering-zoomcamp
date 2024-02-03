-- How many taxi trips were totally made on September 18th 2019?
SELECT COUNT(*)
FROM public.green_taxi_trips
WHERE DATE(lpep_pickup_datetime) = '2019-09-18' AND DATE(lpep_dropoff_datetime) = '2019-09-18';

-- Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
SELECT DATE(lpep_pickup_datetime)
FROM public.green_taxi_trips
WHERE trip_distance = (
	SELECT MAX(trip_distance)
	FROM public.green_taxi_trips
);

-- Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknow
-- Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
SELECT tzl."Borough", SUM(total_amount)
FROM public.green_taxi_trips gtt LEFT JOIN public.taxi_zone_lookup tzl
ON gtt."PULocationID" = tzl."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-09-18'
GROUP BY 1
HAVING SUM(total_amount) >= 50000
ORDER BY 2 DESC;

-- For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
SELECT do_tzl."Zone"
FROM public.green_taxi_trips gtt LEFT JOIN public.taxi_zone_lookup pu_tzl
ON gtt."PULocationID" = pu_tzl."LocationID"
LEFT JOIN public.taxi_zone_lookup do_tzl
ON gtt."DOLocationID" = do_tzl."LocationID"
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-09-01' AND '2019-09-30' AND pu_tzl."Zone" = 'Astoria'
AND tip_amount = (
	SELECT MAX(tip_amount)
	FROM public.green_taxi_trips gtt LEFT JOIN public.taxi_zone_lookup tzl
	ON gtt."PULocationID" = tzl."LocationID"
	WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-09-01' AND '2019-09-30' AND tzl."Zone" = 'Astoria'
);