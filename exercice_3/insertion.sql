ALTER TABLE nyc_taxi.t_taxi_jaune
	ADD COLUMN primary_key text;


UPDATE nyc_taxi.t_taxi_jaune
SET primary_key =
		concat_ws(
				'_',
				tpep_pickup_datetime,
				tpep_dropoff_datetime,
				"PULocationID",
				"DOLocationID"
			);