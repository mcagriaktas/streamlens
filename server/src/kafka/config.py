"""SSL certificate handling and Kafka client configuration."""
import hashlib
import logging
import os
import subprocess
import tempfile
import time
import httpx
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ssl_pem_cache: dict[str, tuple[tempfile.TemporaryDirectory, str | None, str | None, str | None]] = {}


def _ssl_java_to_pem(cluster: dict) -> tuple[str | None, str | None, str | None]:
    """Convert Java truststore/keystore (JKS/PKCS12) to PEM for librdkafka.

    Returns (ca_pem_path, cert_pem_path, key_pem_path) or (None, None, None).
    """
    trust_loc = cluster.get("sslTruststoreLocation") or cluster.get("ssl_truststore_location")
    trust_pw = cluster.get("sslTruststorePassword") or cluster.get("ssl_truststore_password")
    key_loc = cluster.get("sslKeystoreLocation") or cluster.get("ssl_keystore_location")
    key_type = (cluster.get("sslKeystoreType") or cluster.get("ssl_keystore_type") or "pkcs12").strip().lower()
    key_pw = cluster.get("sslKeystorePassword") or cluster.get("ssl_keystore_password")
    key_pass = cluster.get("sslKeyPassword") or cluster.get("ssl_key_password") or key_pw

    if not trust_loc and not key_loc:
        return (None, None, None)

    cache_key = hashlib.sha256(
        f"{trust_loc}:{trust_pw}:{key_loc}:{key_type}:{key_pw}".encode()
    ).hexdigest()
    if cache_key in _ssl_pem_cache:
        _dir, ca, cert, key = _ssl_pem_cache[cache_key]
        return (ca, cert, key)

    tmp = tempfile.TemporaryDirectory(prefix="streamlens_ssl_")
    ca_path: str | None = None
    cert_path: str | None = None
    key_path: str | None = None

    try:
        if trust_loc and Path(trust_loc).exists():
            ca_path = _export_truststore(tmp.name, trust_loc, trust_pw)

        if key_loc and Path(key_loc).exists() and key_type in ("pkcs12", "p12"):
            cert_path, key_path = _export_keystore(tmp.name, key_loc, key_pw, key_pass)

        _ssl_pem_cache[cache_key] = (tmp, ca_path, cert_path, key_path)
        return (ca_path, cert_path, key_path)
    except Exception as e:
        logger.warning("SSL Java->PEM conversion failed: %s", e)
        return (None, None, None)


def _export_truststore(tmp_dir: str, trust_loc: str, trust_pw: str | None) -> str | None:
    """Export all CA certs from a Java truststore to a single PEM file."""
    ca_path = os.path.join(tmp_dir, "ca.pem")
    try:
        list_result = subprocess.run(
            ["keytool", "-list", "-keystore", trust_loc, "-storepass", trust_pw or "", "-v"],
            capture_output=True, text=True, timeout=10,
        )
        if list_result.returncode != 0:
            logger.warning("keytool list truststore failed: %s", (list_result.stderr or list_result.stdout or "")[:200])
            return None

        aliases = _parse_truststore_aliases(list_result.stdout or "")
        if not aliases:
            aliases = _parse_truststore_aliases_short(trust_loc, trust_pw)

        if not aliases:
            subprocess.run(
                ["keytool", "-exportcert", "-rfc", "-keystore", trust_loc, "-storepass", trust_pw or "", "-file", ca_path],
                check=True, capture_output=True, timeout=10,
            )
        else:
            with open(ca_path, "w") as out:
                for i, alias in enumerate(aliases):
                    part = os.path.join(tmp_dir, f"ca_{i}.pem")
                    try:
                        exp = subprocess.run(
                            ["keytool", "-exportcert", "-rfc", "-keystore", trust_loc, "-storepass", trust_pw or "", "-alias", alias, "-file", part],
                            capture_output=True, timeout=10,
                        )
                        if exp.returncode == 0 and os.path.exists(part):
                            with open(part) as f:
                                out.write(f.read())
                            out.write("\n")
                    except Exception as e:
                        logger.debug("keytool export alias %s: %s", alias, e)
            if not os.path.exists(ca_path) or os.path.getsize(ca_path) == 0:
                logger.warning("Truststore export produced empty ca.pem (aliases: %s)", aliases)
                return None

        return ca_path
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning("keytool truststore export failed: %s", e)
        return None


def _parse_truststore_aliases(stdout: str) -> list[str]:
    aliases = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.lower().startswith("alias name:"):
            aliases.append(line.split(":", 1)[1].strip())
    return list(dict.fromkeys(aliases))


def _parse_truststore_aliases_short(trust_loc: str, trust_pw: str | None) -> list[str]:
    result = subprocess.run(
        ["keytool", "-list", "-keystore", trust_loc, "-storepass", trust_pw or ""],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        return []
    aliases = []
    for line in (result.stdout or "").splitlines():
        line = line.strip()
        if line and "contains" not in line and "Keystore" not in line and "," in line:
            aliases.append(line.split(",")[0].strip())
    return list(dict.fromkeys(aliases))


def _export_keystore(
    tmp_dir: str, key_loc: str, key_pw: str | None, key_pass: str | None,
) -> tuple[str | None, str | None]:
    """Export client cert and key from a PKCS12 keystore to PEM files."""
    cert_path = os.path.join(tmp_dir, "cert.pem")
    key_path = os.path.join(tmp_dir, "key.pem")
    pass_arg = (key_pw or key_pass or "").replace("'", "'\\''")
    try:
        subprocess.run(
            ["openssl", "pkcs12", "-in", key_loc, "-out", cert_path, "-nodes", "-clcerts", "-passin", f"pass:{pass_arg}"],
            check=True, capture_output=True, timeout=10,
        )
        subprocess.run(
            ["openssl", "pkcs12", "-in", key_loc, "-out", key_path, "-nodes", "-nocerts", "-passin", f"pass:{pass_arg}"],
            check=True, capture_output=True, timeout=10,
        )
        return cert_path, key_path
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning("openssl keystore export failed: %s", e)
        return None, None


def client_config(cluster: dict) -> dict:
    """Build Kafka client config dict from a cluster definition.

    Supports PEM paths (sslCaLocation, etc.) or Java truststore/keystore
    (automatically converted to PEM for librdkafka).
    """
    bootstrap = (cluster.get("bootstrapServers") or cluster.get("bootstrap_servers") or "").strip()
    bootstrap_list = [s.strip() for s in bootstrap.split(",") if s.strip()]
    cfg: dict[str, Any] = {"bootstrap.servers": ",".join(bootstrap_list)}

    protocol = cluster.get("securityProtocol") or cluster.get("security_protocol") or ""
    if protocol:
        cfg["security.protocol"] = protocol.strip()

    _apply_ssl_endpoint_id(cfg, cluster)
    skip_verify = _apply_ssl_verification(cfg, cluster)
    _apply_ssl_certs(cfg, cluster, skip_verify)
    _apply_sasl(cfg, cluster)
    
    return cfg


def _apply_ssl_endpoint_id(cfg: dict, cluster: dict) -> None:
    ep_algo = None
    if "sslEndpointIdentificationAlgorithm" in cluster:
        ep_algo = cluster["sslEndpointIdentificationAlgorithm"]
    elif "ssl_endpoint_identification_algorithm" in cluster:
        ep_algo = cluster["ssl_endpoint_identification_algorithm"]
    if ep_algo is not None:
        val = str(ep_algo).strip()
        if val == "" or val.lower() == "none":
            val = "none"
        cfg["ssl.endpoint.identification.algorithm"] = val
        logger.info("SSL endpoint identification algorithm: %r", val)


def _apply_ssl_verification(cfg: dict, cluster: dict) -> bool:
    skip = (
        cluster.get("enableSslCertificateVerification") is False
        or cluster.get("enable_ssl_certificate_verification") is False
    )
    if skip:
        cfg["enable.ssl.certificate.verification"] = "false"
        logger.info("SSL certificate verification disabled (insecure; dev only)")
    return skip


def _apply_ssl_certs(cfg: dict, cluster: dict, skip_verify: bool) -> None:
    protocol = cfg.get("security.protocol", "")

    ca_explicit = (
        cluster.get("sslCaLocation")
        or cluster.get("ssl_ca_location")
        or cluster.get("ssl.ca.location")
    )
    ca_pem, cert_pem, key_pem = _ssl_java_to_pem(cluster)

    if ca_explicit:
        cfg["ssl.ca.location"] = ca_explicit.strip()
        logger.info("SSL CA from sslCaLocation: %s", ca_explicit.strip())
    elif ca_pem:
        cfg["ssl.ca.location"] = ca_pem
        logger.info("SSL CA from truststore: %s", ca_pem)

    if protocol and protocol.upper() in ("SSL", "SASL_SSL") and "ssl.ca.location" not in cfg and not skip_verify:
        logger.warning(
            "SSL is enabled but no CA is configured. "
            "Set sslTruststoreLocation or sslCaLocation, or enableSslCertificateVerification=false for dev."
        )

    if cert_pem and key_pem:
        cfg["ssl.certificate.location"] = cert_pem
        cfg["ssl.key.location"] = key_pem
        key_pw = cluster.get("sslKeyPassword") or cluster.get("ssl_key_password")
        if key_pw:
            cfg["ssl.key.password"] = key_pw
    else:
        cert = cluster.get("sslCertificateLocation") or cluster.get("ssl_certificate_location")
        if cert:
            cfg["ssl.certificate.location"] = cert.strip()
        key = cluster.get("sslKeyLocation") or cluster.get("ssl_key_location")
        if key:
            cfg["ssl.key.location"] = key.strip()
        key_pw = cluster.get("sslKeyPassword") or cluster.get("ssl_key_password")
        if key_pw:
            cfg["ssl.key.password"] = key_pw


_VALID_SASL_MECHANISMS = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"}

def _apply_sasl(cfg: dict, cluster: dict) -> None:
    """Apply SASL authentication settings (PLAIN, SCRAM, OAUTHBEARER)."""
    mechanism = cluster.get("saslMechanism") or cluster.get("sasl_mechanism")
    if mechanism:
        value = mechanism.strip()
        if value not in _VALID_SASL_MECHANISMS:
            raise ValueError(
                f"Invalid saslMechanism: {value!r}. "
                f"Must be one of: {', '.join(sorted(_VALID_SASL_MECHANISMS))}"
            )
        cfg["sasl.mechanism"] = value
        logger.info("SASL mechanism: %r", value)

    username = cluster.get("saslUsername") or cluster.get("sasl_username")
    if username:
        cfg["sasl.username"] = username.strip()

    password = cluster.get("saslPassword") or cluster.get("sasl_password")
    if password:
        cfg["sasl.password"] = password

    if mechanism == "OAUTHBEARER":
        _apply_sasl_oauthbearer(cfg, cluster)


def _apply_sasl_oauthbearer(cfg: dict, cluster: dict) -> None:
    """Apply OAuthBearer / OIDC settings.

    Uses a Python ``oauth_cb`` callback (httpx) to fetch tokens instead of
    librdkafka's built-in OIDC, which has OpenSSL 3.0 compatibility issues
    (``STORE routines::unregistered scheme``).

    When OIDC credentials (clientId + clientSecret + tokenEndpointUrl) are
    present, the callback handles the token fetch.  Otherwise falls back to
    raw ``sasl.oauthbearer.config`` passthrough.
    """
    client_id = (
        cluster.get("saslOauthbearerClientId")
        or cluster.get("sasl_oauthbearer_client_id")
    )
    client_secret = (
        cluster.get("saslOauthbearerClientSecret")
        or cluster.get("sasl_oauthbearer_client_secret")
    )
    token_url = (
        cluster.get("saslOauthbearerTokenEndpointUrl")
        or cluster.get("sasl_oauthbearer_token_endpoint_url")
    )

    if client_id and client_secret and token_url:
        scope = (
            cluster.get("saslOauthbearerScope")
            or cluster.get("sasl_oauthbearer_scope")
        )
        # Re-use the CA cert that _apply_ssl_certs already resolved for the
        # Kafka broker connection — works for the token endpoint too.
        ca_location = cfg.get("ssl.ca.location")
        skip_verify = cfg.get("enable.ssl.certificate.verification") == "false"

        cfg["oauth_cb"] = _make_oauth_cb(
            token_url, client_id, client_secret, scope,
            ca_location, skip_verify,
        )
        logger.info(
            "OAuthBearer: using Python callback for token endpoint %s", token_url,
        )
        return

    # Fallback: pass raw oauthbearer config/method to librdkafka
    oauthbearer_config = (
        cluster.get("saslOauthbearerConfig")
        or cluster.get("sasl_oauthbearer_config")
    )
    if oauthbearer_config:
        cfg["sasl.oauthbearer.config"] = str(oauthbearer_config).strip()

    method = (
        cluster.get("saslOauthbearerMethod")
        or cluster.get("sasl_oauthbearer_method")
    )
    if method:
        cfg["sasl.oauthbearer.method"] = str(method).strip()


def _make_oauth_cb(
    token_url: str,
    client_id: str,
    client_secret: str,
    scope: str | None,
    ca_location: str | None,
    skip_verify: bool,
):
    """Return a ``oauth_cb`` callable for confluent-kafka-python.

    The callback fetches an access token from the OIDC provider via
    ``httpx`` (client-credentials grant) and returns ``(token, expiry)``.
    """
    def oauth_cb(config_str: str):  # noqa: ARG001 — required by confluent-kafka
        data: dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if scope:
            data["scope"] = scope

        # SSL verification: use CA pem if available, else system default,
        # or disable entirely when the cluster says so.
        verify: bool | str = True
        if skip_verify:
            verify = False
        elif ca_location:
            verify = ca_location

        try:
            resp = httpx.post(token_url, data=data, verify=verify, timeout=10)
            resp.raise_for_status()
            token_data = resp.json()
            access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 300)
            expiry = time.time() + float(expires_in)
            logger.info("OAuthBearer: token fetched, expires in %ss", expires_in)
            return access_token, expiry
        except Exception as e:
            logger.error("OAuthBearer: token fetch from %s failed: %s", token_url, e)
            raise

    return oauth_cb