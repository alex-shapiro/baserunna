import json, sys, time, urllib.request
base = sys.argv[1]; rows = int(sys.argv[2])
def call(method, path, body=None, token=None):
    req = urllib.request.Request(base + path, method=method, data=None if body is None else body.encode())
    req.add_header("content-type", "application/json")
    if token: req.add_header("authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=120) as r: return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e: return e.code, json.loads(e.read() or b"{}")
DAY = 86_400_000; T0 = 1_700_000_000_000 // DAY * DAY
def ev(i): return '{"t":%d,"u":"u%07d","e":"e%d"}' % (T0 + (i * 7919) % DAY, i % 100000, i % 8)
s, j = call("POST", "/v1/auth/signup", json.dumps({"email": "a@oom.test", "password": "password-123"})); tok = j["token"]
print("signup", s); print("db", call("POST", "/v1/dbs", json.dumps({"name": "m"}), tok)[0])
print("table", call("POST", "/v1/dbs/m/events", json.dumps({"name": "ev", "columns": [{"name": "t", "type": "time"}, {"name": "u", "type": "string"}, {"name": "e", "type": "string"}], "time_column": "t", "order_by": ["t"]}), tok)[0])
per = rows // 7
for p in range(7):
    print("part", p, call("POST", "/v1/dbs/m/events/ev/rows", "[" + ",".join(ev(i) for i in range(p * per, (p + 1) * per)) + "]", tok)[0])
print("trigger", call("POST", "/v1/dbs/m/events/ev/rows", "[" + ev(rows) + "]", tok)[0])
for _ in range(60):
    time.sleep(1)
    s, info = call("GET", "/v1/dbs/m/events/ev", None, tok)
    n = len(info.get("parts", []))
    if n < 8: break
print("parts after wait", n, "healthz", call("GET", "/healthz")[0])
for name, body in [("count by u", {"group_by": ["u"], "aggregates": {"n": {"fn": "count"}}}),
                   ("rows by u", {"columns": ["t", "u", "e"], "order_by": ["u"], "limit": 10000})]:
    body.update({"from": T0, "to": T0 + DAY})
    t = time.time(); s, q = call("POST", "/v1/dbs/m/events/ev/rows/query", json.dumps(body), tok)
    print("query", name, s, "%.2f s" % (time.time() - t), json.dumps(q)[:160])
print("healthz after queries", call("GET", "/healthz")[0])
