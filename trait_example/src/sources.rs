use std::{borrow::Cow, collections::HashMap, io::Cursor};

use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE},
    Client, RequestBuilder,
};
use polars::prelude::*;

use crate::errors::Result;

/// A data source that can load a Polars `DataFrame`.
#[async_trait]
pub trait Source {
    async fn load_data(&self) -> Result<DataFrame>;
}

#[derive(Clone, Debug)]
pub enum SourceKind<'a> {
    Parquet(Cow<'a, str>),
    Postgres(Cow<'a, str>),
    Http {
        url: Cow<'a, str>,
        headers: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
        query: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
        bearer_token: Option<Cow<'a, str>>,
        standard_auth: Option<(Cow<'a, str>, Cow<'a, str>)>,
    },
}

impl<'a> SourceKind<'a> {
    pub fn read_postgres(conn: impl Into<Cow<'a, str>>) -> Self {
        Self::Postgres(conn.into())
    }

    pub fn read_parquet(path: impl Into<Cow<'a, str>>) -> Self {
        Self::Parquet(path.into())
    }

    /// Minimal ctor: only URL; other HTTP options default to `None`.
    pub fn read_http(url: impl Into<Cow<'a, str>>) -> Self {
        Self::Http {
            url: url.into(),
            headers: None,
            query: None,
            bearer_token: None,
            standard_auth: None,
        }
    }

    /// Full ctor with provided options.
    pub fn read_http_with(
        url: impl Into<Cow<'a, str>>,
        headers: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
        query: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
        bearer_token: Option<Cow<'a, str>>,
        standard_auth: Option<(Cow<'a, str>, Cow<'a, str>)>,
    ) -> Self {
        Self::Http {
            url: url.into(),
            headers,
            query,
            bearer_token,
            standard_auth,
        }
    }

    /// Builder entrypoint: `SourceKind::http("url").header(...).query(...).build()`
    pub fn http(url: impl Into<Cow<'a, str>>) -> HttpBuilder<'a> {
        HttpBuilder::new(url)
    }
}

#[async_trait]
impl<'a> Source for SourceKind<'a> {
    async fn load_data(&self) -> Result<DataFrame> {
        match self {
            SourceKind::Parquet(path) => {
                let file = std::fs::File::open(path.as_ref())?;
                Ok(ParquetReader::new(file).finish()?)
            }
            SourceKind::Postgres(_conn) => {
                // placeholder demo frame
                Ok(df!["a" => &[1, 2, 3], "b" => &[4, 5, 6]]?)
            }
            SourceKind::Http {
                url,
                headers,
                query,
                bearer_token,
                standard_auth,
            } => {
                let req = http_builder(
                    url.clone(),
                    headers.clone(),
                    query.clone(),
                    bearer_token.clone(),
                    standard_auth.clone(),
                )?;
                http_request_to_df(req).await
            }
        }
    }
}

/// Ergonomic builder for `SourceKind::Http`.
#[derive(Clone, Debug)]
pub struct HttpBuilder<'a> {
    url: Cow<'a, str>,
    headers: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
    query: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
    bearer_token: Option<Cow<'a, str>>,
    standard_auth: Option<(Cow<'a, str>, Cow<'a, str>)>,
}

impl<'a> HttpBuilder<'a> {
    pub fn new(url: impl Into<Cow<'a, str>>) -> Self {
        Self {
            url: url.into(),
            headers: None,
            query: None,
            bearer_token: None,
            standard_auth: None,
        }
    }

    pub fn header(mut self, key: impl Into<Cow<'a, str>>, val: impl Into<Cow<'a, str>>) -> Self {
        let map = self.headers.get_or_insert_with(HashMap::new);
        map.insert(key.into(), val.into());
        self
    }

    pub fn query(mut self, key: impl Into<Cow<'a, str>>, val: impl Into<Cow<'a, str>>) -> Self {
        let map = self.query.get_or_insert_with(HashMap::new);
        map.insert(key.into(), val.into());
        self
    }

    pub fn bearer(mut self, token: impl Into<Cow<'a, str>>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    pub fn basic(
        mut self,
        user: impl Into<Cow<'a, str>>,
        pass: impl Into<Cow<'a, str>>,
    ) -> Self {
        self.standard_auth = Some((user.into(), pass.into()));
        self
    }

    pub fn build(self) -> SourceKind<'a> {
        SourceKind::Http {
            url: self.url,
            headers: self.headers,
            query: self.query,
            bearer_token: self.bearer_token,
            standard_auth: self.standard_auth,
        }
    }
}

/// Build a `GET` request with optional headers/query/auth.
pub fn http_builder<'a>(
    url: impl Into<Cow<'a, str>>,
    headers: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
    query: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
    bearer_token: Option<Cow<'a, str>>,
    standard_auth: Option<(Cow<'a, str>, Cow<'a, str>)>,
) -> Result<RequestBuilder> {
    let client = Client::new();
    let mut req = client.get(url.into().as_ref());

    // headers
    if let Some(h) = headers {
        let mut hm = HeaderMap::new();
        for (k, v) in h {
            let name = HeaderName::from_bytes(k.as_ref().as_bytes())?;
            let value = HeaderValue::from_str(v.as_ref())?;
            hm.insert(name, value);
        }
        req = req.headers(hm);
    }

    // query ?a=1&b=2
    if let Some(q) = query {
        let qvec: Vec<(String, String)> =
            q.into_iter().map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
        req = req.query(&qvec);
    }

    // auth
    if let Some(token) = bearer_token {
        req = req.bearer_auth(token.as_ref());
    }
    if let Some((user, pass)) = standard_auth {
        req = req.basic_auth(user.as_ref(), Some(pass.as_ref()));
    }

    Ok(req)
}

/// Fetch JSON/NDJSON and parse into a `DataFrame`, then flatten nested structs.
pub async fn http_request_to_df(req: RequestBuilder) -> Result<DataFrame> {
    let res = req.send().await?.error_for_status()?;
    let mut df = DataFrame::empty();

    // own content-type before consuming the body
    let ctype = res
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let bytes = res.bytes().await?;

    // NDJSON (one object per line)
    if ctype.contains("ndjson") || ctype.contains("jsonlines") {
        df = JsonReader::new(Cursor::new(bytes))
            .with_json_format(JsonFormat::JsonLines)
            .finish()?;
        return Ok(normalize_unknown(&df)?);
    }

    // Regular JSON: array or single object → wrap as array
    let val: serde_json::Value = serde_json::from_slice(&bytes)?;
    let array_val = match val {
        serde_json::Value::Array(_) => val,
        serde_json::Value::Object(_) => serde_json::Value::Array(vec![val]),
        _ => serde_json::Value::Null,
    };

    let arr_bytes = serde_json::to_vec(&array_val)?;
    df = JsonReader::new(Cursor::new(arr_bytes))
        .with_json_format(JsonFormat::Json)
        .finish()?;

    Ok(normalize_unknown(&df)?)
}

/// Flatten nested columns without touching Utf8 JSON strings:
/// - unnest all `Struct` columns
/// - explode `List<Struct>` then unnest them
/// Repeats until no nested columns remain.
pub fn normalize_unknown(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut out = df.clone();

    loop {
        // collect names first (end the immutable borrow before mutating)
        let (to_unnest, to_explode_then_unnest): (Vec<String>, Vec<String>) = {
            let mut unnest = Vec::new();
            let mut explode_then = Vec::new();

            for s in out.get_columns() {
                match s.dtype() {
                    DataType::Struct(_) => unnest.push(s.name().to_string()),
                    DataType::List(inner) if matches!(inner.as_ref(), DataType::Struct(_)) => {
                        explode_then.push(s.name().to_string())
                    }
                    _ => {}
                }
            }
            (unnest, explode_then)
        };

        if to_unnest.is_empty() && to_explode_then_unnest.is_empty() {
            break;
        }

        if !to_explode_then_unnest.is_empty() {
            let cols: Vec<&str> = to_explode_then_unnest.iter().map(|s| s.as_str()).collect();
            out = out.explode(cols.clone())?;
            out = out.unnest(cols)?;
        }
        if !to_unnest.is_empty() {
            let cols: Vec<&str> = to_unnest.iter().map(|s| s.as_str()).collect();
            out = out.unnest(cols)?;
        }
    }

    Ok(out)
}
