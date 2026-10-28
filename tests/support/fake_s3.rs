//! An in-process S3 endpoint for testing `CloudStore` without credentials.
//!
//! It implements the subset of the S3 API that `object_store` uses (object `PUT`, `GET`,
//! `DELETE` with conditional and range headers, and `ListObjectsV2`), pages listings in
//! small pages, and injects faults queued with [`FakeS3::fault`].

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri, header};
use axum::response::Response;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const BUCKET: &str = "bucket";
const PAGE: usize = 3;

#[derive(Clone, Debug)]
pub enum Fault {
    /// Responds with this status without handling the request.
    Fail(u16),
    /// Handles the request, then responds with this status as if the response were lost.
    ApplyThenFail(u16),
    /// Waits before handling the request.
    Delay(Duration),
}

struct Obj {
    data: Bytes,
    etag: String,
    modified_s: u64,
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, Obj>,
    faults: VecDeque<(Method, Fault)>,
    requests: BTreeMap<String, usize>,
    writes: u64,
}

#[derive(Clone, Default)]
pub struct FakeS3 {
    inner: Arc<Mutex<Inner>>,
    pub url: String,
}

impl FakeS3 {
    pub async fn start() -> FakeS3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let s = FakeS3 { inner: Default::default(), url: format!("http://{}", listener.local_addr().unwrap()) };
        let app = axum::Router::new().fallback(handle).with_state(s.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        s
    }

    /// Settings that point a `CloudStore` at this endpoint.
    pub fn config(&self) -> Vec<(String, String)> {
        [
            ("aws_endpoint", self.url.as_str()),
            ("aws_allow_http", "true"),
            ("aws_access_key_id", "test"),
            ("aws_secret_access_key", "test"),
            ("aws_region", "auto"),
            ("aws_virtual_hosted_style_request", "false"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    /// Applies `fault` to the next request with `method`.
    pub fn fault(&self, method: Method, fault: Fault) {
        self.inner.lock().unwrap().faults.push_back((method, fault));
    }

    /// Number of requests received with `method`.
    pub fn requests(&self, method: &Method) -> usize {
        self.inner.lock().unwrap().requests.get(method.as_str()).copied().unwrap_or(0)
    }

    pub fn data(&self, key: &str) -> Option<Bytes> {
        self.inner.lock().unwrap().objects.get(key).map(|o| o.data.clone())
    }
}

fn now_s() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn civil(secs: u64) -> (i64, u32, u32, u64) {
    let days = (secs / 86_400) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d, secs % 86_400)
}

fn http_date(secs: u64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let (y, m, d, t) = civil(secs);
    let wd = DAYS[((secs / 86_400) % 7) as usize];
    format!("{wd}, {d:02} {} {y} {:02}:{:02}:{:02} GMT", MONTHS[m as usize - 1], t / 3600, t / 60 % 60, t % 60)
}

fn iso_date(secs: u64) -> String {
    let (y, m, d, t) = civil(secs);
    format!("{y}-{m:02}-{d:02}T{:02}:{:02}:{:02}.000Z", t / 3600, t / 60 % 60, t % 60)
}

fn status(code: u16, body: &str) -> Response {
    Response::builder().status(code).body(Body::from(body.to_string())).unwrap()
}

fn error(code: u16, s3_code: &str) -> Response {
    status(
        code,
        &format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{s3_code}</Code><Message>{s3_code}</Message></Error>"
        ),
    )
}

fn query(uri: &Uri) -> BTreeMap<String, String> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (decode(k), decode(v))
        })
        .collect()
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

async fn handle(State(s): State<FakeS3>, method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    let fault = {
        let mut g = s.inner.lock().unwrap();
        *g.requests.entry(method.to_string()).or_default() += 1;
        match g.faults.iter().position(|(m, _)| *m == method) {
            Some(i) => g.faults.remove(i).map(|(_, f)| f),
            None => None,
        }
    };
    match fault {
        Some(Fault::Fail(code)) => return error(code, "InjectedFault"),
        Some(Fault::Delay(d)) => tokio::time::sleep(d).await,
        _ => {}
    }
    let resp = serve(&s, &method, &uri, &headers, body);
    match fault {
        Some(Fault::ApplyThenFail(code)) => error(code, "InternalError"),
        _ => resp,
    }
}

fn serve(s: &FakeS3, method: &Method, uri: &Uri, headers: &HeaderMap, body: Bytes) -> Response {
    let path = decode(uri.path().trim_start_matches('/'));
    let Some(key) = path.strip_prefix(BUCKET).map(|k| k.trim_start_matches('/').to_string()) else {
        return error(404, "NoSuchBucket");
    };
    let header = |h: header::HeaderName| headers.get(h).and_then(|v| v.to_str().ok()).map(str::to_string);
    let mut g = s.inner.lock().unwrap();
    if key.is_empty() {
        let q = query(uri);
        if *method == Method::POST && q.contains_key("delete") {
            return delete_objects(&mut g, &body);
        }
        return list(&g, &q);
    }
    match *method {
        Method::PUT => {
            let cur = g.objects.get(&key).map(|o| o.etag.clone());
            if header(header::IF_NONE_MATCH).as_deref() == Some("*") && cur.is_some() {
                return error(412, "PreconditionFailed");
            }
            if let Some(want) = header(header::IF_MATCH) {
                match cur {
                    None => return error(404, "NoSuchKey"),
                    Some(c) if c != want => return error(412, "PreconditionFailed"),
                    _ => {}
                }
            }
            g.writes += 1;
            let digest = baserunna::util::sha256(&body);
            let etag = format!("\"{}\"", baserunna::util::b64::hex(&digest[..16]));
            g.objects.insert(key, Obj { data: body, etag: etag.clone(), modified_s: now_s() });
            Response::builder().status(200).header(header::ETAG, etag).body(Body::empty()).unwrap()
        }
        Method::DELETE => {
            g.objects.remove(&key);
            status(204, "")
        }
        Method::GET | Method::HEAD => {
            let Some(o) = g.objects.get(&key) else { return error(404, "NoSuchKey") };
            let size = o.data.len() as u64;
            if header(header::IF_NONE_MATCH).as_deref() == Some(o.etag.as_str()) {
                return Response::builder().status(304).header(header::ETAG, &o.etag).body(Body::empty()).unwrap();
            }
            let b = Response::builder()
                .header(header::ETAG, &o.etag)
                .header(header::LAST_MODIFIED, http_date(o.modified_s));
            match header(header::RANGE) {
                None => b.status(200).header(header::CONTENT_LENGTH, size).body(Body::from(o.data.clone())).unwrap(),
                Some(r) => {
                    let (a, z) = r.trim_start_matches("bytes=").split_once('-').unwrap();
                    let (a, z): (u64, u64) = (a.parse().unwrap(), z.parse().unwrap());
                    if a >= size {
                        return error(416, "InvalidRange");
                    }
                    let z = z.min(size - 1);
                    b.status(206)
                        .header(header::CONTENT_LENGTH, z - a + 1)
                        .header(header::CONTENT_RANGE, format!("bytes {a}-{z}/{size}"))
                        .body(Body::from(o.data.slice(a as usize..z as usize + 1)))
                        .unwrap()
                }
            }
        }
        _ => error(405, "MethodNotAllowed"),
    }
}

/// `DeleteObjects`, which `object_store` uses for single deletes as well.
fn delete_objects(g: &mut Inner, body: &[u8]) -> Response {
    let body = String::from_utf8_lossy(body);
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult>");
    for part in body.split("<Key>").skip(1) {
        let key = part.split("</Key>").next().unwrap_or("");
        let key = key.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&");
        g.objects.remove(&key);
        xml += &format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(&key));
    }
    xml += "</DeleteResult>";
    Response::builder().status(200).header(header::CONTENT_TYPE, "application/xml").body(Body::from(xml)).unwrap()
}

fn list(g: &Inner, q: &BTreeMap<String, String>) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let after = q.get("continuation-token").or(q.get("start-after")).cloned().unwrap_or_default();
    let max = q.get("max-keys").and_then(|m| m.parse().ok()).unwrap_or(PAGE).min(PAGE);
    let keys: Vec<(&String, &Obj)> =
        g.objects.range(after.clone()..).filter(|(k, _)| **k > after && k.starts_with(&prefix)).take(max + 1).collect();
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    for (k, o) in keys.iter().take(max) {
        xml += &format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size></Contents>",
            xml_escape(k),
            iso_date(o.modified_s),
            xml_escape(&o.etag),
            o.data.len()
        );
    }
    if keys.len() > max {
        xml += &format!(
            "<IsTruncated>true</IsTruncated><NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(keys[max - 1].0)
        );
    } else {
        xml += "<IsTruncated>false</IsTruncated>";
    }
    xml += "</ListBucketResult>";
    Response::builder().status(200).header(header::CONTENT_TYPE, "application/xml").body(Body::from(xml)).unwrap()
}

#[test]
fn dates_are_formatted_like_s3() {
    assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
    assert_eq!(http_date(1_793_137_899), "Tue, 27 Oct 2026 21:51:39 GMT");
    assert_eq!(iso_date(951_782_400), "2000-02-29T00:00:00.000Z");
}
