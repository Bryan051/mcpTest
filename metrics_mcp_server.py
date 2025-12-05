from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from mcp.server.fastmcp import FastMCP


# ============================================================
# 1. DB 연결 유틸
# ============================================================

DB_HOST = os.getenv("DB_HOST", "192.168.1.69")   # cp-node IP
DB_PORT = int(os.getenv("DB_PORT", "30432"))     # postgres-nodeport
DB_NAME = os.getenv("DB_NAME", "postgresdb")
DB_USER = os.getenv("DB_USER", "test")
DB_PASSWORD = os.getenv("DB_PASSWORD", "test")


def get_conn():
    """
    PostgreSQL 커넥션을 하나 생성해서 반환합니다.
    - RealDictCursor 덕분에 row를 dict처럼 접근 가능 (row["col"])
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


# ============================================================
# 2. MCP 서버 런타임 생성
# ============================================================

# FastMCP = MCP 서버 런타임
# "metrics-mcp"가 Host/LLM 쪽에서 보게 될 MCP 서버 이름
mcp = FastMCP("metrics-mcp")


# ============================================================
# 3. MCP Tools 정의
#    - @mcp.tool() 로 등록된 모든 함수가
#      tools/list 에서 보이는 도구가 됨
#    - 파라미터/리턴 타입 → 자동으로 JSON Schema 생성
# ============================================================


@mcp.tool()
def get_metrics_summary(
    service_name: Optional[str] = None,
    minutes: int = 10,
) -> List[Dict[str, Any]]:
    """
    최근 N분 동안 서비스별 평균 TPS, 최대 TPS, 평균 에러율, 평균 P95 지연(ms)을 조회합니다.

    - service_name:
        * 지정하면: 해당 서비스만 조회 (결과는 길이 0 또는 1인 리스트)
        * 지정하지 않으면: 전체 서비스에 대해 GROUP BY로 집계
    - minutes: 몇 분 동안의 데이터를 집계할지 (기본 10분)
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

    return [
        {
            "service_name": row["service_name"],
            "minutes": minutes,
            "avg_tps": row["avg_tps"],
            "max_tps": row["max_tps"],
            "avg_error_rate": row["avg_error_rate"],
            "avg_latency_p95": row["avg_latency_p95"],
        }
        for row in rows
    ]


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

    return [
        {
            "service_name": r["service_name"],
            "timestamp": r["log_ts"].isoformat(),
            "level": r["level"],
            "code": r["code"],
            "message": r["message"],
        }
        for r in rows
    ]


@mcp.tool()
def get_hop_trace(trace_id: str) -> List[Dict[str, Any]]:
    """
    hop_trace 테이블에서 특정 trace_id의 hop 경로를 순서대로 조회합니다.
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


@mcp.tool()
def get_all_hop_traces(limit: int = 100) -> List[Dict[str, Any]]:
    """
    hop_trace 테이블의 모든 hop 경로를 조회합니다. 양이 많을 수 있어 개수 제한이 있습니다.
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


# ============================================================
# 4. MCP 서버 실행 엔트리포인트
# ============================================================

if __name__ == "__main__":
    # stdio 기반 MCP 서버 실행
    # → Host(Gemini CLI, 나중엔 우리 Gateway)가
    #    이 프로세스를 띄우고 stdin/stdout으로 JSON-RPC 통신하게 됨.
    mcp.run(transport="stdio")
