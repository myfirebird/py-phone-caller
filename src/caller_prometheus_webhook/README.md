# Caller Prometheus Webhook

Alertmanager-compatible webhook that turns Prometheus alerts into phone calls
and SMS notifications.

## Responsibilities
- Accept Alertmanager payloads and dispatch notifications.
- Support call-only, SMS-only, SMS-before-call, and call-and-SMS workflows.
- Use asyncio queues to throttle and process notifications.
- Support Machine-to-Machine (M2M) OAuth2 Bearer token authentication via Keycloak while maintaining 100% backward-compatibility for direct, unauthenticated LAN environments.

## HTTP API
Routes are configured in `settings.toml` under `[caller_prometheus_webhook]`:
- `POST /<prometheus_webhook_app_route_call_only>` (default: `/call_only`, alias `/call`)
- `POST /<prometheus_webhook_app_route_sms_only>` (default: `/sms_only`, alias `/sms`)
- `POST /<prometheus_webhook_app_route_sms_before_call>` (default: `/sms_before_call`)
- `POST /<prometheus_webhook_app_route_call_and_sms>` (default: `/call_and_sms`)

When deployed behind Apache APISIX Gateway:
- `POST /api/v1/alerts/<route>`

## Prometheus Alertmanager Integration Examples

### 1. Direct Webhook (Standalone / Without APISIX or Keycloak)

Use this configuration when running in an isolated LAN or trusted internal network without an authentication gateway:

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'cluster', 'service']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'py-phone-caller'
  routes:
    - match:
        severity: critical
      receiver: 'py-phone-caller-call-and-sms'
    - match:
        severity: warning
      receiver: 'py-phone-caller-sms-only'

receivers:
  # Emergency Phone Call Only
  - name: 'py-phone-caller'
    webhook_configs:
      - url: 'http://192.168.1.100:8084/call_only'
        send_resolved: false

  # Simultaneous Phone Call & SMS
  - name: 'py-phone-caller-call-and-sms'
    webhook_configs:
      - url: 'http://192.168.1.100:8084/call_and_sms'
        send_resolved: false

  # SMS Only
  - name: 'py-phone-caller-sms-only'
    webhook_configs:
      - url: 'http://192.168.1.100:8084/sms_only'
        send_resolved: false
```

---

### 2. Enterprise M2M Webhook (With Apache APISIX & Keycloak OAuth2)

Modern versions of Prometheus Alertmanager support native OAuth2 client credentials grant via `http_config.oauth2`. Alertmanager automatically acquires, caches, and refreshes the Bearer token:

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'severity']
  receiver: 'py-phone-caller-m2m'

receivers:
  - name: 'py-phone-caller-m2m'
    webhook_configs:
      - url: 'https://alerts.example.com/api/v1/alerts/call_only'
        send_resolved: false
        http_config:
          oauth2:
            client_id: 'py-phone-caller-m2m'
            client_secret: '<YOUR_KEYCLOAK_M2M_SECRET>'
            token_url: 'https://alerts.example.com/realms/py-phone-caller/protocol/openid-connect/token'
            scopes: ['email', 'profile']
            tls_config:
              insecure_skip_verify: false
```

> [!TIP]
> The secret for `py-phone-caller-m2m` is generated automatically during deployment and can be displayed using `./show-credentials.sh` on the control node or retrieved from `/etc/py-phone-caller/credentials.env` on the server.

---

## Configuration & Settings

Configured in `src/config/settings.toml`:

```toml
[caller_prometheus_webhook]
prometheus_webhook_port = 8084
prometheus_webhook_app_route_call_only = "call_only"
prometheus_webhook_app_route_sms_only = "sms_only"
prometheus_webhook_app_route_sms_before_call = "sms_before_call"
prometheus_webhook_app_route_call_and_sms = "call_and_sms"
prometheus_webhook_receivers = ["+39123456789"]
require_auth = false   # Set to true to enforce M2M Bearer token validation
webhook_tts_lang = ""  # TTS voice language for webhook calls, e.g. "z" = Kokoro Mandarin
```

### TTS voice language

`message` (or the Alertmanager `annotations.description`) is spoken verbatim
after the callee answers. Which voice renders it is resolved once per request:

1. `?lang=` / `?language=` query parameter on the webhook call.
2. The duty group's own `lang` in `config/probe_config.yml`
   (only when the request targets a group via `?group_id=` / `?group=`).
3. `[caller_prometheus_webhook] webhook_tts_lang` in `settings.toml`.
4. Nothing — the `generate_audio` service keeps its own default engine
   (`[generate_audio] tts_engine` / `*_language_code`), which is the pre-1.2
   behaviour.

The resolved code is forwarded to `asterisk_caller /place_call?lang=` and ends
up in the generated wav name, so a Chinese announcement must not rely on the
default Spanish/Kokoro voice being installed. Example for a Chinese Zabbix
alert:

```bash
curl -X POST 'http://HOST:8084/call?group_id=<group id>&token=<group token>&lang=z' \
  -H 'Content-Type: application/json' \
  -d '{"message":"灾难告警。web01 CPU 使用率 大于等于 百分之95。"}'
```

## Running Locally

```bash
export CALLER_CONFIG_DIR=src/config
uv run python -m caller_prometheus_webhook.caller_prometheus_webhook
```

## Docker Container

```bash
docker build -t caller-prometheus-webhook -f src/caller_prometheus_webhook/Dockerfile .
```

## Calling the duty-group webhook from Zabbix

The duty-group endpoint accepts any of these bodies, so a Zabbix webhook media
type can pass the alert text straight through:

| Sender | Body | Result |
| --- | --- | --- |
| Prometheus Alertmanager | `{"alerts":[{"status":"firing","annotations":{"description":"..."}}]}` | firing descriptions only |
| Zabbix / generic JSON | `{"subject":"PROBLEM: ...","message":"{ALERT.MESSAGE}"}` | `message` (falls back to `subject`, `body`, `content`, `description`, `title`) |
| Zabbix script media / curl | raw text (`Content-Type: text/plain`) | whole body becomes the message |
| Zabbix form media | `message=...&subject=...` (`application/x-www-form-urlencoded`) | `message` |

Example Zabbix media type parameters:

```
URL:      https://<host>:8084/call?group=<duty_group>&token=<group token>
Method:   POST
Body:     {"subject": "{ALERT.SUBJECT}", "message": "{ALERT.MESSAGE}"}
Headers:  Content-Type: application/json
```

Use `/call`, `/sms`, `/sms_before_call` or `/call_and_sms` in the URL - the
suffix selects the channel mix, and resolved alerts are never dispatched.

### Endpoint aliases (both are supported - do not remove)

| Canonical (settings route) | Short alias | Notification mode |
| --- | --- | --- |
| `/call_only` | `/call` | voice call only |
| `/sms_only` | `/sms` | SMS only |
| `/sms_before_call` | — | SMS first, then call |
| `/call_and_sms` | `/alert`, `/webhook` | call + SMS |
| — | `/probe`, `/probe_trigger` | trigger one probe run (see caller_prober README) |

The canonical names come from `prometheus_webhook_app_route_*` in
`settings.toml` and are what the deploy templates (HAProxy ACLs, APISIX
gateway, Alertmanager configs) reference; the short aliases are what the
prober UI generates because `notification_type` values are `call`/`sms`. Both
sets are live and must stay registered - deleting either breaks an integration
that is documented elsewhere in this repository.
