mod data;
mod s3;

use aws_sdk_s3::types::ByteStream;
use data::{get_data, process};
use s3::upload_bytes;

use chrono::Utc;
use sqlx::PgPool;

#[tokio::main]
async fn main() {
    let client = s3::get_client();

    let pool = PgPool::connect("postgres://postgres:password@localhost:5432")
        .await
        .unwrap();

    let rows = get_data(&pool).await.unwrap();

    let files = process(rows);

    for (id, content) in files {
        let filename = get_filename(id);
        upload_bytes(&client, "cron", &filename, ByteStream::from(content))
            .await
            .unwrap();
    }

    let res = client
        .list_objects_v2()
        .bucket("cron")
        .send()
        .await
        .unwrap();

    for object in res.contents.unwrap_or_default() {
        println!("Found: {:?}", object.key);
    }
}

fn get_filename(id: String) -> String {
    let now = Utc::now();
    let time = now.format("%Y-%m-%dT%H:%M").to_string();

    format!("{time} {id}.txt")
}
