use std::time::Duration;

use crate::error::{Error, Result};
use crate::retryable::{self, IsRetryable};
use crate::slow_warn::warn_if_slow;

const SLOW_REQUEST_THRESHOLD: Duration = Duration::from_secs(30);

pub async fn request(
    client: &reqwest::Client,
    req_builder: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
) -> Result<reqwest::Response> {
    let resp = retryable::run(
        || async {
            let request = req_builder(client).build()?;
            let url = request.url().clone();
            let resp = warn_if_slow(
                &|| format!("HTTP request to {url}"),
                SLOW_REQUEST_THRESHOLD,
                client.execute(request),
            )
            .await?;
            let Err(err) = resp.error_for_status_ref() else {
                return Ok(resp);
            };

            let is_retryable = err.is_retryable();

            let mut error: Error = err.into();
            let body = resp.text().await?;
            if !body.is_empty() {
                error = error.context(format!("Error message body:\n{body}"));
            }

            Err(retryable::Error {
                error,
                is_retryable,
            })
        },
        &retryable::HEAVY_LOADED_OPTIONS,
    )
    .await?;
    Ok(resp)
}
