use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::{FromRow, PgPool};

#[derive(FromRow)]
pub(crate) struct DevicePower {
    device_id: String,
    hour: NaiveDateTime,
    power: f64,
}

#[derive(Serialize)]
struct Power {
    hour: NaiveDateTime,
    power: f64,
}

impl From<DevicePower> for Power {
    fn from(device: DevicePower) -> Self {
        Self {
            hour: device.hour,
            power: device.power,
        }
    }
}

pub(crate) async fn get_data(pool: &PgPool) -> Result<Vec<DevicePower>> {
    let result = sqlx::query_as::<_, DevicePower>("SELECT device_id, date_trunc('hour', timestamp) AS hour, SUM(power) AS power FROM power GROUP BY device_id, date_trunc('hour', timestamp)")
        .fetch_all(pool)
        .await
        ?;

    Ok(result)
}

pub(crate) fn process(data: Vec<DevicePower>) -> impl Iterator<Item = (String, Vec<u8>)> {
    let grouped = data.into_iter().fold(HashMap::new(), |mut map, current| {
        let powers = map.entry(current.device_id.clone()).or_insert(vec![]);
        powers.push(Power::from(current));

        map
    });

    grouped.into_iter().map(|(id, data)| {
        let mut wtr = csv::Writer::from_writer(vec![]);

        for row in data {
            wtr.serialize(row).unwrap();
        }

        let content = wtr.into_inner().unwrap();

        (id, content)
    })
}
