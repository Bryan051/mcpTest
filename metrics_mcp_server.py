# metrics_mcp_server.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from mcp.server.fastmcp import FastMCP


# ---- DB 설정: 환경변수에서 읽기 ----

DB_HOST = os.getenv("DB_HOST", "192.168.1.69")  # cp-node IP
DB_PORT = int(os.getenv("DB_PORT", "30432"))   # postgres-nodeport
DB_NAME = os.getenv("DB_NAME", "postgresdb")
DB_USER = os.getenv("DB_USER", "test")
DB_PASSWORD = os.getenv("DB_PASSWORD", "test")


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


mcp = FastMCP("metrics-mcp")


# ---- Tool 1: 서비스별 TPS/지연 요약 ----

@mcp.tool()
def get_metrics_summary(
    service_name: Optional[str] = None,
    minutes: int = 10
) -> List[Dict[str, Any]]:
    """
    최근 N분 동안 서비스별 평균 TPS, 최대 TPS, 평균 에러율, 평균 P95 지연(ms)을 조회합니다.

    - service_name:
        * 지정하면: 해당 서비스만 조회 (결과는 길이 0 또는 1인 리스트)
        * 지정하지 않으면: 전체 서비스에 대해 GROUP BY로 집계
    - minutes: 몇 분 동안의 데이터를 집계할지 (기본 10분)

    반환 예시 (service_name 미지정 시):

    [
      {
        "service_name": "order-api",
        "minutes": 10,
        "avg_tps": ...,
        "max_tps": ...,
        "avg_error_rate": ...,
        "avg_latency_p95": ...
      },
      {
        "service_name": "quote-stream",
        ...
      }
    ]
    """
    conn = get_conn()
    cur = conn.cursor()

    base_sql = """
        SELECT
          service_name,
          AVG(tps)::float         AS avg_tps,
          MAX(tps)::float         AS max_tps,
          AVG(error_rate)::float  AS avg_error_rate,
          AVG(latency_p95)::float AS avg_latency_p95
        FROM service_metrics
        WHERE bucket_ts >= (NOW() - (%s || ' minutes')::interval)
    """

    params: List[Any] = [minutes]

    if service_name:
        base_sql += " AND service_name = %s"
        params.append(service_name)

    base_sql += """
        GROUP BY service_name
        ORDER BY service_name
    """

    cur.execute(base_sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        # 데이터 없을 때도 LLM이 다루기 편하게 통일된 형태로 반환
        return [
            {
                "service_name": service_name or "<all>",
                "minutes": minutes,
                "avg_tps": 0.0,
                "max_tps": 0.0,
                "avg_error_rate": 0.0,
                "avg_latency_p95": 0.0,
                "note": "해당 기간에 데이터가 없습니다.",
            }
        ]

    results: List[Dict[str, Any]] = []
    for row in rows:
        results.append(
            {
                "service_name": row["service_name"],
                "minutes": minutes,
                "avg_tps": row["avg_tps"],
                "max_tps": row["max_tps"],
                "avg_error_rate": row["avg_error_rate"],
                "avg_latency_p95": row["avg_latency_p95"],
            }
        )

    return results


# ---- Tool 2: 이벤트/에러 로그 검색 ----

@mcp.tool()
def search_event_logs(
    keyword: str,
    service_name: Optional[str] = None,
    level: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    event_logs 테이블에서 message/코드에 keyword가 포함된 로그를 검색합니다.

    - keyword: 메시지/코드에 포함될 키워드
    - service_name: 특정 서비스로 필터링 (옵션)
    - level: INFO / WARN / ERROR 등 로그 레벨 필터 (옵션)
    - limit: 최대 결과 개수
    """
    conn = get_conn()
    cur = conn.cursor()

    conditions = ["(message ILIKE %s OR code ILIKE %s)"]
    params: List[Any] = [f"%{keyword}%", f"%{keyword}%"]

    if service_name:
        conditions.append("service_name = %s")
        params.append(service_name)
    if level:
        conditions.append("level = %s")
        params.append(level)

    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT service_name, log_ts, level, code, message
        FROM event_logs
        WHERE {where_clause}
        ORDER BY log_ts DESC
        LIMIT %s
    """
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    results: List[Dict[str, Any]] = []
    for r in rows:
        results.append(
            {
                "service_name": r["service_name"],
                "timestamp": r["log_ts"].isoformat(),
                "level": r["level"],
                "code": r["code"],
                "message": r["message"],
            }
        )

    return results


# ---- Tool 3: 특정 trace_id의 hop 경로 조회 ----

@mcp.tool()
def get_hop_trace(trace_id: str) -> List[Dict[str, Any]]:
    """
    hop_trace 테이블에서 특정 trace_id의 hop 경로를 순서대로 조회합니다.

    - trace_id: 트랜잭션/요청 단위의 trace 식별자
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT trace_id, hop_index, node_name, hop_ts, latency_ms, status
        FROM hop_trace
        WHERE trace_id = %s
        ORDER BY hop_index ASC
        """,
        (trace_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "trace_id": r["trace_id"],
            "hop_index": r["hop_index"],
            "node_name": r["node_name"],
            "timestamp": r["hop_ts"].isoformat(),
            "latency_ms": float(r["latency_ms"]),
            "status": r["status"],
        }
        for r in rows
    ]


# ---- Tool 4: 모든 hop_trace 조회 ----

@mcp.tool()
def get_all_hop_traces(limit: int = 100) -> List[Dict[str, Any]]:
    """
    hop_trace 테이블의 모든 hop 경로를 조회합니다. 양이 많을 수 있어 개수 제한이 있습니다.

    - limit: 최대 결과 개수 (기본 100)
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT trace_id, hop_index, node_name, hop_ts, latency_ms, status
        FROM hop_trace
        ORDER BY hop_ts DESC, trace_id, hop_index ASC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "trace_id": r["trace_id"],
            "hop_index": r["hop_index"],
            "node_name": r["node_name"],
            "timestamp": r["hop_ts"].isoformat(),
            "latency_ms": float(r["latency_ms"]),
            "status": r["status"],
        }
        for r in rows
    ]


if __name__ == "__main__":
    # stdio 기반 MCP 서버 실행
    mcp.run(transport="stdio")
