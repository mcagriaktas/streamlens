import os
import random
from typing import Any

from .kafka import kafka_service

DEFAULT_MAX_TOPICS = 2000
DEFAULT_TOPIC_PAGE_SIZE = 50


def _collect_connected_topics(state: dict) -> set[str]:
    """Identify topics referenced by producers, consumers, streams, connectors, or ACLs."""
    connected: set[str] = set()
    for s in state["streams"]:
        connected.update((s.get("consumesFrom") or []) + (s.get("producesTo") or []))
    for p in state["producers"]:
        connected.update(p.get("producesTo") or [])
    for c in state["consumers"]:
        connected.update(c.get("consumesFrom") or [])
    for c in state.get("connectors", []):
        t = c.get("topic")
        if t and t != "?":
            connected.add(t)
    for a in state.get("acls", []):
        t = a.get("topic")
        if t:
            connected.add(t)
    return connected


def _select_topics_to_show(
    all_broker_topics: set[str], connected: set[str], max_topics: int,
) -> tuple[set[str], dict | None]:
    """Choose which topics to include, sampling if the cluster exceeds max_topics."""
    total = len(all_broker_topics)
    if total <= max_topics:
        return set(all_broker_topics), None

    connected_in_broker = connected & all_broker_topics
    others = list(all_broker_topics - connected_in_broker)
    sample_size = max(0, max_topics - len(connected_in_broker))
    shown_others = set(random.sample(others, min(sample_size, len(others)))) if others else set()
    topics_to_show = connected_in_broker | shown_others
    meta = {"totalTopicCount": total, "shownTopicCount": len(topics_to_show)}
    return topics_to_show, meta


def _build_topic_nodes(state: dict, topics_to_show: set[str]) -> tuple[list[dict], set[str]]:
    """Create topic nodes and return (nodes, topic_names set)."""
    nodes = []
    topic_names = set(topics_to_show)

    for t in state["topics"]:
        name = t["name"]
        if name not in topics_to_show:
            continue
        nodes.append({
            "id": f"topic:{name}",
            "type": "topic",
            "data": {"label": name, "details": t},
        })

    return nodes, topic_names


def _ensure_referenced_topics(
    state: dict, nodes: list[dict], topic_names: set[str],
) -> None:
    """Add placeholder topic nodes for topics referenced by other entities but missing from broker metadata."""
    def _ensure(name: str):
        if name and name not in topic_names:
            topic_names.add(name)
            nodes.append({"id": f"topic:{name}", "type": "topic", "data": {"label": name, "details": None}})

    for s in state["streams"]:
        for name in s["consumesFrom"] + s["producesTo"]:
            _ensure(name)
    for p in state["producers"]:
        for name in p.get("producesTo", []):
            _ensure(name)
    for c in state["consumers"]:
        for name in c.get("consumesFrom", []):
            _ensure(name)
    for a in state.get("acls", []):
        _ensure(a.get("topic"))


def _clean_label(label: str, prefixes: list[tuple[str, str]]) -> str:
    for prefix, replacement in prefixes:
        if label.startswith(prefix):
            return label.replace(prefix, replacement, 1)
    return label


_PRODUCER_EDGE_STYLES: dict[str, tuple[dict, bool]] = {
    "jmx":               ({"strokeDasharray": "2,2"}, True),
    "offset":            ({"strokeDasharray": "3,3"}, True),
    "prometheus":        ({"strokeDasharray": "2,2"}, True),
    "prometheus-broker": ({"strokeDasharray": "2,2"}, True),
}


def _build_producer_nodes(state: dict) -> tuple[list[dict], list[dict]]:
    nodes, edges = [], []
    for p in state["producers"]:
        source = p.get("source", "unknown")
        label = _clean_label(p.get("label") or p["id"], [
            ("app:", ""),
            ("prometheus:", ""),
            ("prometheus-broker:active-producer:", "Active → "),
            ("jmx:active-producer:", "Active → "),
            ("offset:active-producer:", "Active → "),
        ])

        data: dict[str, Any] = {"label": label, "source": source, "principal": p.get("principal")}
        if p.get("clientId"):
            data["clientId"] = p["clientId"]

        nodes.append({
            "id": p["id"],
            "type": "producer",
            "data": data,
        })

        style, animated = _PRODUCER_EDGE_STYLES.get(source, ({}, True))
        for topic in p["producesTo"]:
            edges.append({
                "id": f"{p['id']}->{topic}",
                "source": p["id"],
                "target": f"topic:{topic}",
                "type": "produces",
                "animated": animated,
                "style": style,
            })

    return nodes, edges


def _build_consumer_nodes(state: dict) -> tuple[list[dict], list[dict]]:
    nodes, edges = [], []
    for c in state["consumers"]:
        source = c.get("source", "unknown")
        label = _clean_label(c["id"], [("app:", ""), ("group:", "")])

        nodes.append({
            "id": c["id"],
            "type": "consumer",
            "data": {"label": label, "source": source},
        })
        for topic in c["consumesFrom"]:
            edges.append({
                "id": f"{topic}->{c['id']}",
                "source": f"topic:{topic}",
                "target": c["id"],
                "type": "consumes",
                "animated": True,
            })

    return nodes, edges


def _build_stream_edges(state: dict) -> list[dict]:
    edges = []
    for s in state["streams"]:
        app_label = s.get("label", s["id"].replace("app:", "", 1))
        for in_topic in s.get("consumesFrom") or []:
            for out_topic in s.get("producesTo") or []:
                edges.append({
                    "id": f"streams:{s['id']}:{in_topic}:{out_topic}",
                    "source": f"topic:{in_topic}",
                    "target": f"topic:{out_topic}",
                    "type": "streams",
                    "label": app_label,
                    "animated": True,
                    "style": {"strokeDasharray": "8,4"},
                })
    return edges


def _build_connector_nodes(state: dict) -> tuple[list[dict], list[dict]]:
    nodes, edges = [], []
    seen: set[str] = set()
    for c in state["connectors"]:
        if c["id"] not in seen:
            seen.add(c["id"])
            connector_label = c["id"].replace("connect:", "", 1) if c["id"].startswith("connect:") else c["id"]
            nodes.append({"id": c["id"], "type": "connector", "data": {"label": connector_label, "type": c["type"]}})

        topic = c.get("topic") or "?"
        if topic != "?":
            if c["type"] == "sink":
                edges.append({"id": f"{topic}->{c['id']}", "source": f"topic:{topic}", "target": c["id"], "type": "sinks", "animated": True})
            else:
                edges.append({"id": f"{c['id']}->{topic}", "source": c["id"], "target": f"topic:{topic}", "type": "sources", "animated": True})

    return nodes, edges


def _build_acl_nodes(state: dict) -> tuple[list[dict], list[dict]]:
    nodes, edges = [], []

    topic_acls: dict[str, list[dict[str, Any]]] = {}
    for a in state.get("acls", []):
        topic = a.get("topic")
        if not topic:
            continue
        topic_acls.setdefault(topic, []).append({
            "principal": a.get("principal", "") or "",
            "host": a.get("host", "") or "",
            "operation": a.get("operation", "") or "?",
            "permissionType": a.get("permissionType", "") or "?",
        })

    for topic, acl_list in topic_acls.items():
        if not acl_list:
            continue
        acl_id = f"acl:topic:{topic}"
        count = len(acl_list)
        nodes.append({
            "id": acl_id,
            "type": "acl",
            "data": {"label": f"ACL ({count})" if count > 1 else "ACL", "topic": topic, "acls": acl_list},
        })
        edges.append({
            "id": f"{acl_id}->topic:{topic}",
            "source": acl_id,
            "target": f"topic:{topic}",
            "type": "acl",
            "animated": False,
            "style": {"strokeDasharray": "5,5", "stroke": "#b45309"},
        })

    return nodes, edges


def _build_schema_nodes(state: dict, existing_nodes: list[dict]) -> tuple[list[dict], list[dict]]:
    """Create schema nodes grouped by Schema Registry ID.

    When multiple subjects share the same schema ID (same content), a single
    node is created and connected to all related topic nodes.
    """
    nodes, edges = [], []
    topic_node_ids = {n["id"] for n in existing_nodes if n.get("type") == "topic"}

    schema_id_groups: dict[Any, list[dict]] = {}
    for s in state["schemas"]:
        sid = s.get("id")
        topic_name = s.get("topicName")
        if not topic_name or f"topic:{topic_name}" not in topic_node_ids:
            continue
        if sid is None:
            sid = f"_subj_{s['subject']}"
        schema_id_groups.setdefault(sid, []).append(s)

    for sid, group in schema_id_groups.items():
        subjects = [g["subject"] for g in group]
        topic_names_for_schema = list(dict.fromkeys(g["topicName"] for g in group))
        first = group[0]
        node_id = f"schema:{sid}"

        schema_type = first.get("type", "AVRO")
        label = subjects[0] if len(subjects) == 1 else "Multiple subjects"
        sub_label = schema_type if len(subjects) == 1 else f"{schema_type} · {len(subjects)} subject(s)"

        nodes.append({
            "id": node_id,
            "type": "schema",
            "data": {
                "label": label,
                "subLabel": sub_label,
                "schemaType": schema_type,
                "subjects": subjects,
                "subject": subjects[0],
                "version": first["version"],
                "schemaId": sid,
            },
        })

        for tn in topic_names_for_schema:
            edges.append({
                "id": f"{tn}->{node_id}",
                "source": f"topic:{tn}",
                "target": node_id,
                "type": "schema_link",
                "style": {"strokeDasharray": "3,3", "stroke": "#60a5fa"},
            })

    return nodes, edges


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_topology(cluster_id: int, cluster: dict) -> dict:
    """Build topology graph from real cluster state.

    When the cluster has more than TOPOLOGY_MAX_TOPICS topics, only a subset
    is included (all connected topics + a random sample of the rest).
    """
    try:
        state = kafka_service.fetch_system_state(cluster)
    except Exception:
        state = kafka_service._empty_state()

    all_broker_topics = {t["name"] for t in state["topics"]}

    max_topics = int(os.environ.get("TOPOLOGY_MAX_TOPICS", str(DEFAULT_MAX_TOPICS)))
    if max_topics < 1:
        max_topics = DEFAULT_MAX_TOPICS

    connected = _collect_connected_topics(state)
    topics_to_show, meta = _select_topics_to_show(all_broker_topics, connected, max_topics)

    nodes, topic_names = _build_topic_nodes(state, topics_to_show)
    _ensure_referenced_topics(state, nodes, topic_names)

    p_nodes, p_edges = _build_producer_nodes(state)
    c_nodes, c_edges = _build_consumer_nodes(state)
    s_edges = _build_stream_edges(state)
    conn_nodes, conn_edges = _build_connector_nodes(state)
    acl_nodes, acl_edges = _build_acl_nodes(state)
    schema_nodes, schema_edges = _build_schema_nodes(state, nodes)

    nodes.extend(p_nodes + c_nodes + conn_nodes + acl_nodes + schema_nodes)
    edges = p_edges + c_edges + s_edges + conn_edges + acl_edges + schema_edges

    result: dict[str, Any] = {"nodes": nodes, "edges": edges}
    if meta is not None:
        result["_meta"] = meta
    return result


def paginate_topology_data(data: dict, offset: int = 0, limit: int = DEFAULT_TOPIC_PAGE_SIZE) -> dict:
    """Return a paginated slice of topic nodes from the full topology snapshot.

    Connected topics (those with producers/consumers/connectors/streams/ACLs) come first,
    then standalone topics alphabetically.  Non-topic nodes are included only when they
    have at least one edge to a visible topic.  Edges are filtered to visible endpoints.
    """
    all_nodes: list[dict] = data.get("nodes") or []
    all_edges: list[dict] = data.get("edges") or []

    topic_nodes: list[dict] = []
    non_topic_nodes: list[dict] = []
    for n in all_nodes:
        (topic_nodes if n.get("type") == "topic" else non_topic_nodes).append(n)

    connected_topic_ids: set[str] = set()
    for e in all_edges:
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if src.startswith("topic:"):
            if not tgt.startswith("topic:"):
                connected_topic_ids.add(src)
            else:
                connected_topic_ids.update((src, tgt))
        elif tgt.startswith("topic:"):
            connected_topic_ids.add(tgt)

    def _sort_key(n: dict):
        is_connected = 0 if n["id"] in connected_topic_ids else 1
        label = (n.get("data") or {}).get("label", "")
        return (is_connected, label.lower())

    topic_nodes.sort(key=_sort_key)

    total_topics = len(topic_nodes)
    page_topics = topic_nodes[offset : offset + limit]
    page_topic_ids = {n["id"] for n in page_topics}

    non_topic_to_topics: dict[str, set[str]] = {}
    for e in all_edges:
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if src.startswith("topic:") and not tgt.startswith("topic:"):
            non_topic_to_topics.setdefault(tgt, set()).add(src)
        elif tgt.startswith("topic:") and not src.startswith("topic:"):
            non_topic_to_topics.setdefault(src, set()).add(tgt)

    visible_non_topic = [
        n for n in non_topic_nodes
        if non_topic_to_topics.get(n["id"], set()) & page_topic_ids
    ]

    visible_ids = page_topic_ids | {n["id"] for n in visible_non_topic}
    visible_edges = [
        e for e in all_edges
        if str(e.get("source", "")) in visible_ids and str(e.get("target", "")) in visible_ids
    ]

    return {
        "nodes": page_topics + visible_non_topic,
        "edges": visible_edges,
        "_meta": {
            "totalTopicCount": total_topics,
            "loadedTopicCount": min(offset + limit, total_topics),
            "offset": offset,
            "limit": limit,
            "hasMore": offset + limit < total_topics,
        },
    }


def search_topology(data: dict, query: str) -> dict:
    """Search ALL nodes in the full topology snapshot by label, id, or type.

    Returns matching nodes together with their directly-connected non-topic nodes
    and the edges that link them.  Also returns ``matchIds``.
    """
    query_lower = (query or "").lower().strip()
    if not query_lower:
        return {"nodes": [], "edges": [], "matchIds": []}

    all_nodes: list[dict] = data.get("nodes") or []
    all_edges: list[dict] = data.get("edges") or []

    match_ids: set[str] = set()
    for n in all_nodes:
        label = ((n.get("data") or {}).get("label") or "").lower()
        nid = (n.get("id") or "").lower()
        ntype = (n.get("type") or "").lower()
        if query_lower in label or query_lower in nid or query_lower in ntype:
            match_ids.add(n["id"])

    if not match_ids:
        return {"nodes": [], "edges": [], "matchIds": []}

    matching_topic_ids = {mid for mid in match_ids if mid.startswith("topic:")}
    connected_ids: set[str] = set()
    for e in all_edges:
        src = str(e.get("source", ""))
        tgt = str(e.get("target", ""))
        if src in matching_topic_ids and not tgt.startswith("topic:"):
            connected_ids.add(tgt)
        if tgt in matching_topic_ids and not src.startswith("topic:"):
            connected_ids.add(src)

    visible_ids = match_ids | connected_ids
    visible_nodes = [n for n in all_nodes if n["id"] in visible_ids]
    visible_edges = [
        e for e in all_edges
        if str(e.get("source", "")) in visible_ids and str(e.get("target", "")) in visible_ids
    ]

    return {
        "nodes": visible_nodes,
        "edges": visible_edges,
        "matchIds": sorted(match_ids),
    }
