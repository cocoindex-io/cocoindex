use crate::retryable::{self, IsRetryable};

pub async fn request(
    req_builder: impl Fn() -> reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    let resp = retryable::run(
        || async {
            let req = req_builder();
            let resp = req.send().await?;
            let Err(err) = resp.error_for_status_ref() else {
                return Ok(resp);
            };

            let is_retryable = err.is_retryable();

            let mut anyhow_error = anyhow::Error::new(err);
            let body = resp.text().await?;
            if !body.is_empty() {
                anyhow_error = anyhow_error.context(format!("Error message body:\n{body}"));
            }

            Err(retryable::Error {
                error: anyhow_error,
                is_retryable,
            })
        },
        &retryable::HEAVY_LOADED_OPTIONS,
    )
    .await?;
    Ok(resp)
}
