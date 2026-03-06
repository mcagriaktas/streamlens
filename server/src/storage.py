"""
Storage layer: clusters in a JSON file, topology snapshots in memory.
Admin can edit data/clusters.json and restart the server; no database or login required.
"""
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

_SERVER_DIR = Path(__file__).resolve().parent.parent
_CLUSTERS_PATH = Path(os.environ.get("CLUSTERS_JSON", _SERVER_DIR / "data" / "clusters.json"))
_lock = threading.Lock()
_snapshot_cache: dict[int, dict] = {}


def _ensure_clusters_file():
    _CLUSTERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not _CLUSTERS_PATH.exists():
        with _lock:
            _CLUSTERS_PATH.write_text(json.dumps({"clusters": []}, indent=2), encoding="utf-8")


def _read_raw() -> dict:
    """Read full clusters.json (clusters + root-level config)."""
    _ensure_clusters_file()
    with _lock:
        raw = json.loads(_CLUSTERS_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {"clusters": []}
    return dict(raw)


def _read_clusters() -> list[dict]:
    raw = _read_raw()
    clusters = raw.get("clusters")
    if not isinstance(clusters, list):
        return []
    return list(clusters)


def _write_clusters(clusters: list[dict]) -> None:
    _ensure_clusters_file()
    raw = _read_raw()
    raw["clusters"] = clusters
    raw.pop("enableKafkaEventProduceFromUi", None)  # now per-cluster only
    with _lock:
        _CLUSTERS_PATH.write_text(
            json.dumps(raw, indent=2),
            encoding="utf-8",
        )


def _bool_from(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes")


_SECURITY_FIELD_ALIASES: list[tuple[str, ...]] = [
    ("securityProtocol", "security_protocol"),
    ("sslCaLocation", "ssl_ca_location", "ssl.ca.location"),
    ("sslCertificateLocation", "ssl_certificate_location"),
    ("sslKeyLocation", "ssl_key_location"),
    ("sslKeyPassword", "ssl_key_password"),
    ("sslTruststoreLocation", "ssl_truststore_location"),
    ("sslTruststorePassword", "ssl_truststore_password"),
    ("sslKeystoreLocation", "ssl_keystore_location"),
    ("sslKeystoreType", "ssl_keystore_type"),
    ("sslKeystorePassword", "ssl_keystore_password"),
    ("saslMechanism", "sasl_mechanism"),
    ("saslUsername", "sasl_username"),
    ("saslPassword", "sasl_password"),
    ("saslOauthbearerMethod", "sasl_oauthbearer_method"),
    ("saslOauthbearerClientId", "sasl_oauthbearer_client_id"),
    ("saslOauthbearerClientSecret", "sasl_oauthbearer_client_secret"),
    ("saslOauthbearerTokenEndpointUrl", "sasl_oauthbearer_token_endpoint_url"),
]


def _resolve(c: dict, *keys: str):
    """Return the first non-None value found among the given keys."""
    for k in keys:
        val = c.get(k)
        if val is not None:
            return val
    return None


def _has_any_key(c: dict, *keys: str) -> bool:
    return any(k in c for k in keys)


def _cluster_from_row(c: dict) -> dict:
    """Normalize to API shape (camelCase, createdAt). Per-cluster enableKafkaEventProduceFromUi and optional SSL."""
    out = {
        "id": c["id"],
        "name": c["name"],
        "clusterType": _resolve(c, "clusterType", "cluster_type"),
        "bootstrapServers": _resolve(c, "bootstrapServers", "bootstrap_servers") or "",
        "schemaRegistryUrl": _resolve(c, "schemaRegistryUrl", "schema_registry_url"),
        "connectUrl": _resolve(c, "connectUrl", "connect_url"),
        "jmxHost": _resolve(c, "jmxHost", "jmx_host"),
        "jmxPort": _resolve(c, "jmxPort", "jmx_port"),
        "prometheusUrl": _resolve(c, "prometheusUrl", "prometheus_url"),
        "createdAt": _resolve(c, "createdAt", "created_at") or "",
        "enableKafkaEventProduceFromUi": _bool_from(
            _resolve(c, "enableKafkaEventProduceFromUi", "enable_kafka_event_produce_from_ui")
        ),
    }

    for aliases in _SECURITY_FIELD_ALIASES:
        val = _resolve(c, *aliases)
        if val is not None:
            out[aliases[0]] = val

    # These two need special defaults: "" and True respectively, even when the
    # key is present but the value is None/falsy.
    if _has_any_key(c, "sslEndpointIdentificationAlgorithm", "ssl_endpoint_identification_algorithm"):
        out["sslEndpointIdentificationAlgorithm"] = (
            _resolve(c, "sslEndpointIdentificationAlgorithm", "ssl_endpoint_identification_algorithm") or ""
        )
    if _has_any_key(c, "enableSslCertificateVerification", "enable_ssl_certificate_verification"):
        val = _resolve(c, "enableSslCertificateVerification", "enable_ssl_certificate_verification")
        out["enableSslCertificateVerification"] = val if val is not None else True

    return out


def get_clusters() -> list[dict]:
    rows = _read_clusters()
    return [_cluster_from_row(c) for c in rows]


def get_cluster(id: int) -> dict | None:
    clusters = _read_clusters()
    for c in clusters:
        if c.get("id") == id:
            return _cluster_from_row(c)
    return None


def create_cluster(
    name: str,
    bootstrap_servers: str,
    schema_registry_url: str | None = None,
    connect_url: str | None = None,
    jmx_host: str | None = None,
    jmx_port: int | None = None,
    enable_kafka_event_produce_from_ui: bool = False,
) -> dict:
    clusters = _read_clusters()
    next_id = max((c.get("id") or 0 for c in clusters), default=0) + 1
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_cluster = {
        "id": next_id,
        "name": name,
        "bootstrapServers": bootstrap_servers,
        "schemaRegistryUrl": schema_registry_url,
        "connectUrl": connect_url,
        "jmxHost": jmx_host,
        "jmxPort": jmx_port,
        "createdAt": now,
        "enableKafkaEventProduceFromUi": enable_kafka_event_produce_from_ui,
    }
    clusters.append(new_cluster)
    _write_clusters(clusters)
    return _cluster_from_row(new_cluster)


def update_cluster(
    id: int,
    name: str,
    bootstrap_servers: str,
    schema_registry_url: str | None = None,
    connect_url: str | None = None,
    jmx_host: str | None = None,
    jmx_port: int | None = None,
    enable_kafka_event_produce_from_ui: bool | None = None,
) -> dict | None:
    clusters = _read_clusters()
    for i, c in enumerate(clusters):
        if c.get("id") == id:
            existing = clusters[i]
            if enable_kafka_event_produce_from_ui is None:
                enable_kafka_event_produce_from_ui = _bool_from(
                    existing.get("enableKafkaEventProduceFromUi") or existing.get("enable_kafka_event_produce_from_ui")
                )
            updated = {
                "id": id,
                "name": name,
                "bootstrapServers": bootstrap_servers,
                "schemaRegistryUrl": schema_registry_url,
                "connectUrl": connect_url,
                "jmxHost": jmx_host,
                "jmxPort": jmx_port,
                "createdAt": _resolve(existing, "createdAt", "created_at") or "",
                "enableKafkaEventProduceFromUi": enable_kafka_event_produce_from_ui,
            }
            for aliases in _SECURITY_FIELD_ALIASES:
                updated[aliases[0]] = _resolve(existing, *aliases)
            updated["sslEndpointIdentificationAlgorithm"] = _resolve(
                existing, "sslEndpointIdentificationAlgorithm", "ssl_endpoint_identification_algorithm"
            )
            val = _resolve(existing, "enableSslCertificateVerification", "enable_ssl_certificate_verification")
            updated["enableSslCertificateVerification"] = val if val is not None else existing.get("enable_ssl_certificate_verification")
            clusters[i] = updated
            _write_clusters(clusters)
            return _cluster_from_row(clusters[i])
    return None


def delete_cluster(id: int) -> None:
    clusters = _read_clusters()
    clusters = [c for c in clusters if c.get("id") != id]
    _write_clusters(clusters)
    with _lock:
        _snapshot_cache.pop(id, None)


def _strip_url_credentials(url: str | None) -> str | None:
    """Remove userinfo (username:password) from a URL, keeping the rest intact."""
    if not url:
        return url
    try:
        parsed = urlparse(url)
        if not parsed.username:
            return url  # no credentials present
        # Reconstruct without userinfo
        netloc = parsed.hostname or ""
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    except Exception:
        return url  # return as-is if parsing fails


# Fields that should never be sent to the frontend
_SENSITIVE_FIELDS = {
    "sslCaLocation", "sslCertificateLocation", "sslKeyLocation", "sslKeyPassword",
    "sslTruststoreLocation", "sslTruststorePassword", "sslKeystoreLocation",
    "sslKeystoreType", "sslKeystorePassword", "sslEndpointIdentificationAlgorithm",
    "enableSslCertificateVerification", "saslUsername", "saslPassword", "saslMechanism",
    "saslOauthbearerMethod", "saslOauthbearerClientId", "saslOauthbearerClientSecret",
    "saslOauthbearerTokenEndpointUrl",
}


def sanitize_cluster_for_api(cluster: dict) -> dict:
    """Return a copy of the cluster dict safe for the frontend (no credentials, no SSL secrets)."""
    out = dict(cluster)
    out["schemaRegistryUrl"] = _strip_url_credentials(out.get("schemaRegistryUrl"))
    out["connectUrl"] = _strip_url_credentials(out.get("connectUrl"))
    out["bootstrapServers"] = _strip_url_credentials(out.get("bootstrapServers"))
    # Remove SSL key material — the frontend never needs these
    for field in _SENSITIVE_FIELDS:
        out.pop(field, None)
    return out


def get_latest_snapshot(cluster_id: int) -> dict | None:
    with _lock:
        return _snapshot_cache.get(cluster_id)


def create_snapshot(cluster_id: int, data: dict) -> dict:
    snapshot = {
        "id": cluster_id,
        "clusterId": cluster_id,
        "data": data,
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with _lock:
        _snapshot_cache[cluster_id] = snapshot
    return snapshot
