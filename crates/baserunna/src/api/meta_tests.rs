//! Tests for metadata commands through the HTTP API.

use crate::meta::META_FIELD_MAX;
use crate::sim::cost::World;
use baserunna_util::run_paused;
use bytes::Bytes;
use serde_json::json;

#[test]
fn metadata_requests_reject_ids_longer_than_the_field_limit() {
    run_paused(async {
        let w = World::new(1, 1).await;
        let admin = w.signup(0, "admin@x.io").await;
        let r = w.json(0, "POST", "/v1/orgs", &admin, json!({"name": "acme"})).await;
        assert_eq!(r["name"], "acme", "{r}");
        for (len, status, code) in [(META_FIELD_MAX, 404, "not_found"), (META_FIELD_MAX + 1, 400, "invalid_argument")] {
            let id = "a".repeat(len);
            for (method, path, body) in [
                ("DELETE", format!("/v1/me/sessions/{id}"), Bytes::new()),
                ("DELETE", format!("/v1/api-keys/{id}"), Bytes::new()),
                ("POST", format!("/v1/admin/users/{id}/disable"), Bytes::from("{}")),
                ("POST", format!("/v1/orgs/acme/members/{id}"), Bytes::from(r#"{"role":"reader"}"#)),
                ("DELETE", format!("/v1/orgs/acme/members/{id}"), Bytes::new()),
            ] {
                let route = path.replace(&id, "{id}");
                let r = w.call(0, method, &path, &admin, body).await;
                let got = (r.status, r.json()["error"]["code"].clone());
                assert_eq!(got, (status, json!(code)), "{method} {route} with an ID of {len} bytes: {:?}", r.json());
            }
        }
    });
}
