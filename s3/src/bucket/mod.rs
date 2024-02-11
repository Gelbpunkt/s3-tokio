mod presign;
use crate::error::S3Error;
use awscreds::Credentials;
use awsregion::Region;
use bytes::Bytes;
use http::HeaderMap;
use http_body_util::Full;
use hyper_rustls::HttpsConnector;
use hyper_timeout::TimeoutConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub use std::io::Read;

mod credentials;

mod tag;
pub use tag::*;

mod create;
pub use create::*;

mod list;
pub use list::*;

mod delete;

mod copy;

mod get;

mod put;

mod head;

mod utils;

mod client;

pub type Query = HashMap<String, String>;

pub use crate::serde_types::{
    BucketLocationResult, CompleteMultipartUploadData, CorsConfiguration, HeadObjectResult,
    InitiateMultipartUploadResponse, ListBucketResult, ListMultipartUploadsResult, Part,
};
pub(crate) use crate::utils::error_from_response_data;
pub use crate::utils::PutStreamResponse;

pub use crate::request::Request;

pub const CHUNK_SIZE: usize = 8_388_608; // 8 Mebibytes, min is 5 (5_242_880);

/// Instantiate an existing Bucket
///
/// # Example
///
/// ```no_run
/// use s3::bucket::Bucket;
/// use s3::creds::Credentials;
///
/// let bucket_name = "rust-s3-test";
/// let region = "us-east-1".parse().unwrap();
/// let credentials = Credentials::default().unwrap();
///
/// let bucket = Bucket::new(bucket_name, region, credentials);
/// ```
#[derive(Clone, Debug)]
pub struct Bucket {
    pub name: String,
    pub region: Region,
    pub credentials: Arc<RwLock<Credentials>>,
    pub extra_headers: HeaderMap,
    pub extra_query: Query,
    pub request_timeout: Option<Duration>,
    path_style: bool,
    listobjects_v2: bool,
    http_client: Arc<Client<TimeoutConnector<HttpsConnector<HttpConnector>>, Full<Bytes>>>,
}

const DEFAULT_REQUEST_TIMEOUT: Option<Duration> = Some(Duration::from_secs(60));

fn validate_expiry(expiry_secs: u32) -> Result<(), S3Error> {
    if 604800 < expiry_secs {
        return Err(S3Error::MaxExpiry(expiry_secs));
    }
    Ok(())
}

impl Bucket {
    pub fn with_path_style(&self) -> Self {
        Self {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: true,
            listobjects_v2: self.listobjects_v2,
            http_client: self.http_client.clone(),
        }
    }

    pub fn with_extra_headers(&self, extra_headers: HeaderMap) -> Self {
        Self {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            extra_headers,
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            http_client: self.http_client.clone(),
        }
    }

    pub fn with_extra_query(&self, extra_query: HashMap<String, String>) -> Self {
        Self {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query,
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            http_client: self.http_client.clone(),
        }
    }

    pub fn with_request_timeout(&self, request_timeout: Duration) -> Result<Self, S3Error> {
        let http_client = Arc::new(client::create_client(Some(request_timeout))?);

        Ok(Self {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: Some(request_timeout),
            path_style: self.path_style,
            listobjects_v2: self.listobjects_v2,
            http_client,
        })
    }

    pub fn with_listobjects_v1(&self) -> Self {
        Self {
            name: self.name.clone(),
            region: self.region.clone(),
            credentials: self.credentials.clone(),
            extra_headers: self.extra_headers.clone(),
            extra_query: self.extra_query.clone(),
            request_timeout: self.request_timeout,
            path_style: self.path_style,
            listobjects_v2: false,
            http_client: self.http_client.clone(),
        }
    }

    pub(crate) fn _tags_xml<S: AsRef<str>>(&self, tags: &[(S, S)]) -> String {
        let mut s = String::new();
        let content = tags
            .iter()
            .map(|(name, value)| {
                format!(
                    "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                    name.as_ref(),
                    value.as_ref()
                )
            })
            .fold(String::new(), |mut a, b| {
                a.push_str(b.as_str());
                a
            });
        s.push_str("<Tagging><TagSet>");
        s.push_str(&content);
        s.push_str("</TagSet></Tagging>");
        s
    }
}
