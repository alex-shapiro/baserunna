//! Tests for metadata commands through the HTTP API.

use super::META_BODY_MAX;
use crate::meta::ID_MAX;
use crate::sim::cost::World;
use baserunna_util::run_paused;
use bytes::Bytes;
use serde_json::json;

#[test]
fn metadata_api_rejects_ids_longer_than_id_limit() {
    run_paused(async {
        let world = World::new(1, 1).await;
        let admin_token = world.signup(0, "admin@x.io").await;
        let org = world.json(0, "POST", "/v1/orgs", &admin_token, json!({"name": "acme"})).await;
        assert_eq!(org["name"], "acme", "{org}");
        for (id_len, expected_status, expected_code) in
            [(ID_MAX, 404, "not_found"), (ID_MAX + 1, 400, "invalid_argument")]
        {
            let id = "a".repeat(id_len);
            for (method, path, body) in [
                ("DELETE", format!("/v1/me/sessions/{id}"), Bytes::new()),
                ("DELETE", format!("/v1/api-keys/{id}"), Bytes::new()),
                ("POST", format!("/v1/admin/users/{id}/disable"), Bytes::from("{}")),
                ("POST", format!("/v1/orgs/acme/members/{id}"), Bytes::from(r#"{"role":"reader"}"#)),
                ("DELETE", format!("/v1/orgs/acme/members/{id}"), Bytes::new()),
            ] {
                let route = path.replace(&id, "{id}");
                let response = world.call(0, method, &path, &admin_token, body).await;
                let actual = (response.status, response.json()["error"]["code"].clone());
                assert_eq!(
                    actual,
                    (expected_status, json!(expected_code)),
                    "{method} {route} with an ID of {id_len} bytes: {:?}",
                    response.json()
                );
            }
        }
    });
}

#[test]
fn metadata_api_rejects_bodies_over_body_limit() {
    run_paused(async {
        let world = World::new(1, 1).await;
        let token = world.signup(0, "admin@x.io").await;
        for path in ["/v1/api-keys", "/v1/orgs", "/v1/dbs"] {
            let fits = json!({"name": "a".repeat(META_BODY_MAX - 100)}).to_string();
            let response = world.call(0, "POST", path, &token, Bytes::from(fits)).await;
            assert_eq!(response.status, 400, "POST {path} with a body under the limit: {:?}", response.json());
            let too_large = json!({"name": "a".repeat(META_BODY_MAX)}).to_string();
            let response = world.call(0, "POST", path, &token, Bytes::from(too_large)).await;
            assert_eq!(response.status, 413, "POST {path} with a body over the limit: {:?}", response.json());
        }
    });
}
