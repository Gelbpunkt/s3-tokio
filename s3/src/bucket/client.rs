use bytes::Bytes;
use http_body_util::Full;
use hyper_timeout::TimeoutConnector;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;

use crate::{error::S3Error, Bucket};

pub fn create_client(
    request_timeout: Option<std::time::Duration>,
) -> Result<Client<TimeoutConnector<HttpConnector>, Full<Bytes>>, S3Error> {
    let mut timeout_connector = TimeoutConnector::new(HttpConnector::new());
    timeout_connector.set_connect_timeout(request_timeout);
    timeout_connector.set_read_timeout(request_timeout);
    timeout_connector.set_write_timeout(request_timeout);

    Ok(Client::builder(TokioExecutor::new()).build::<_, Full<Bytes>>(timeout_connector))
}

impl Bucket {
    pub fn http_client(&self) -> Client<TimeoutConnector<HttpConnector>, Full<Bytes>> {
        self.http_client.clone()
    }
}
