//! Tests for metadata commands through the HTTP API.

use crate::meta::META_FIELD_MAX_BYTES;
use crate::sim::cost::World;
use baserunna_util::run_paused;
use bytes::Bytes;
use serde_json::json;

#[test]
fn metadata_api_rejects_ids_longer_than_field_limit() {
    run_paused(async {
        let world = World::new(1, 1).await;
        let admin_token = world.signup(0, "admin@x.io").await;
        let org = world.json(0, "POST", "/v1/orgs", &admin_token, json!({"name": "acme"})).await;
        assert_eq!(org["name"], "acme", "{org}");
        for (id_len, expected_status, expected_code) in
            [(META_FIELD_MAX_BYTES, 404, "not_found"), (META_FIELD_MAX_BYTES + 1, 400, "invalid_argument")]
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
