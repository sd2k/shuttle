use anyhow::Result;
use aws_sdk_s3::{config, types::ByteStream, Client, Credentials, Region};

const KEY_ID: &str = "key_id";
const KEY_SECRET: &str = "key_secret";
const REGION: &str = "af-south-1";

/// Get a s3 Client
pub(crate) fn get_client() -> Client {
    let cred = Credentials::new(KEY_ID, KEY_SECRET, None, None, "custom-provider");
    let region = Region::new(REGION);
    let endpoint = aws_sdk_s3::Endpoint::immutable(http::Uri::from_static("http://localhost:4566"));
    let conf_builder = config::Builder::new()
        .region(region)
        .credentials_provider(cred)
        .endpoint_resolver(endpoint);

    let conf = conf_builder.build();

    Client::from_conf(conf)
}

/// Uploads bytes to bucket
pub(crate) async fn upload_bytes(
    client: &Client,
    bucket: &str,
    destination: &str,
    body: ByteStream,
) -> Result<()> {
    let req = client
        .put_object()
        .bucket(bucket)
        .key(destination)
        .body(body);

    req.send().await?;

    Ok(())
}
