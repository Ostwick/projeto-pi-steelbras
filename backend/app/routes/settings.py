import json
import logging
import smtplib
import ssl
from base64 import urlsafe_b64encode
from datetime import date, datetime, time, timezone
from email.message import EmailMessage
from hashlib import sha256

from cryptography.fernet import Fernet, InvalidToken
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_app_db, get_source_db
from app.services.sync_service import BOM_COST_ROLLUP_SQL
from app.schemas import (
    AlertPreviewResponse,
    AlertRecipient,
    AlertSettingsResponse,
    AlertSettingsUpdate,
    AlertDispatchResponse,
    CostVarianceItem,
    ProductAlertItem,
    ProductAlertsResponse,
    RuleHitItem,
    SMTPConfig,
)

router = APIRouter(prefix="/api/settings", tags=["settings"])
logger = logging.getLogger(__name__)


def _build_fernet() -> Fernet:
    raw_key = settings.alerts_secret_key

    if raw_key:
        try:
            return Fernet(raw_key.encode("utf-8"))
        except Exception:
            digest = sha256(raw_key.encode("utf-8")).digest()
            return Fernet(urlsafe_b64encode(digest))

    fallback = (
        f"{settings.postgres_user}|{settings.postgres_password}|"
        f"{settings.smtp_user}|{settings.api_host}"
    )
    digest = sha256(fallback.encode("utf-8")).digest()
    return Fernet(urlsafe_b64encode(digest))


def _encrypt_secret(value: str) -> str:
    return _build_fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def _decrypt_secret(value: str) -> str:
    return _build_fernet().decrypt(value.encode("utf-8")).decode("utf-8")


def _hash_alert_key(
    product_code: str,
    tipo_alerta: str,
    mensagem_alerta: str | None,
    data_referencia: date | None,
    custo_atual: float | None,
    variacao_percentual: float | None,
) -> str:
    raw = "|".join(
        [
            product_code or "",
            tipo_alerta or "",
            mensagem_alerta or "",
            data_referencia.isoformat() if data_referencia else "",
            f"{custo_atual:.6f}" if custo_atual is not None else "",
            f"{variacao_percentual:.6f}" if variacao_percentual is not None else "",
        ]
    )
    return sha256(raw.encode("utf-8")).hexdigest()


def _send_alert_email(
    smtp_server: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
    recipients: list[str],
    subject: str,
    body: str,
) -> tuple[int, int, list[str]]:
    smtp_timeout = 15

    if smtp_port == 465:
        connection = smtplib.SMTP_SSL(
            host=smtp_server,
            port=smtp_port,
            timeout=smtp_timeout,
            context=ssl.create_default_context(),
        )
    else:
        connection = smtplib.SMTP(host=smtp_server, port=smtp_port, timeout=smtp_timeout)
        connection.ehlo()
        connection.starttls(context=ssl.create_default_context())
        connection.ehlo()

    success_count = 0
    fail_count = 0
    failed_recipients: list[str] = []

    with connection:
        connection.login(smtp_user, smtp_pass)

        for recipient in recipients:
            message = EmailMessage()
            message["From"] = smtp_user
            message["To"] = recipient
            message["Subject"] = subject
            message.set_content(body)
            try:
                connection.send_message(message)
                success_count += 1
            except Exception:
                fail_count += 1
                failed_recipients.append(recipient)

    return success_count, fail_count, failed_recipients


def _ensure_alert_tables(app_db: Session) -> None:
    app_db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS alert_settings (
                id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                smtp_server VARCHAR(255),
                smtp_port INTEGER NOT NULL DEFAULT 465,
                smtp_user VARCHAR(255),
                smtp_pass_encrypted TEXT,
                recipients_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                invoice_increase_pct NUMERIC(10,4) NOT NULL DEFAULT 5.0,
                avg_cost_increase_pct NUMERIC(10,4) NOT NULL DEFAULT 3.0,
                avg_cost_vs_last_invoice_pct NUMERIC(10,4) NOT NULL DEFAULT 10.0,
                split_finished_goods BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_by VARCHAR(100) NOT NULL DEFAULT 'system'
            );
            """
        )
    )

    app_db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS alert_settings_history (
                id BIGSERIAL PRIMARY KEY,
                changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                changed_by VARCHAR(100) NOT NULL,
                action VARCHAR(40) NOT NULL,
                before_json JSONB,
                after_json JSONB
            );
            """
        )
    )

    app_db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS alert_rule_state (
                rule_name VARCHAR(100) PRIMARY KEY,
                last_seen_nf_date DATE,
                last_seen_nf_number VARCHAR(100),
                last_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    )

    app_db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS alert_email_runs (
                id BIGSERIAL PRIMARY KEY,
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                requested_by VARCHAR(100) NOT NULL,
                status VARCHAR(30) NOT NULL,
                success_count INTEGER NOT NULL DEFAULT 0,
                fail_count INTEGER NOT NULL DEFAULT 0,
                failed_recipients JSONB,
                error_message TEXT
            );
            """
        )
    )

    app_db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS alert_dispatch_log (
                id BIGSERIAL PRIMARY KEY,
                product_code VARCHAR(100) NOT NULL,
                alert_key VARCHAR(64) NOT NULL,
                alert_type VARCHAR(30),
                data_referencia DATE,
                message TEXT,
                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    )

    app_db.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS alert_dispatch_log_uniq
            ON alert_dispatch_log (product_code, alert_key);
            """
        )
    )

    app_db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS product_bom_alerts (
                id BIGSERIAL PRIMARY KEY,
                produto_codigo VARCHAR(100) NOT NULL,
                tipo_alerta VARCHAR(20) NOT NULL,
                descricao VARCHAR(200),
                custo_anterior NUMERIC(18, 6),
                custo_atual NUMERIC(18, 6),
                variacao_percentual NUMERIC(18, 6),
                mensagem_alerta TEXT,
                severidade VARCHAR(20),
                data_referencia DATE,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    )

    default_recipients = []
    if settings.smtp_user:
        default_recipients = [{"email": settings.smtp_user}]

    encrypted_default_pass = _encrypt_secret(settings.smtp_pass) if settings.smtp_pass else None

    app_db.execute(
        text(
            """
            INSERT INTO alert_settings (
                id,
                smtp_server,
                smtp_port,
                smtp_user,
                smtp_pass_encrypted,
                recipients_json,
                invoice_increase_pct,
                avg_cost_increase_pct,
                avg_cost_vs_last_invoice_pct,
                split_finished_goods,
                updated_by
            ) VALUES (
                1,
                :smtp_server,
                :smtp_port,
                :smtp_user,
                :smtp_pass_encrypted,
                CAST(:recipients_json AS JSONB),
                5.0,
                3.0,
                10.0,
                TRUE,
                'bootstrap'
            )
            ON CONFLICT (id) DO NOTHING;
            """
        ),
        {
            "smtp_server": settings.smtp_server,
            "smtp_port": settings.smtp_port,
            "smtp_user": settings.smtp_user,
            "smtp_pass_encrypted": encrypted_default_pass,
            "recipients_json": json.dumps(default_recipients),
        },
    )
    app_db.commit()


def _to_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_finished_good(product_code: str) -> bool:
    code = (product_code or "").upper()
    return not (code.startswith("MP") or code.startswith("SA") or code.startswith("SE"))


def _to_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return None


def _parse_recipients(raw_recipients: object) -> list[AlertRecipient]:
    if raw_recipients is None:
        return []

    parsed = raw_recipients
    if isinstance(raw_recipients, str):
        try:
            parsed = json.loads(raw_recipients)
        except json.JSONDecodeError:
            parsed = []

    recipients: list[AlertRecipient] = []
    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict) and item.get("email"):
                recipients.append(
                    AlertRecipient(
                        email=str(item["email"]),
                        name=str(item["name"]) if item.get("name") else None,
                    )
                )
    return recipients


def _row_to_alert_settings(row: dict) -> AlertSettingsResponse:
    recipients = _parse_recipients(row.get("recipients_json"))
    return AlertSettingsResponse(
        smtp=SMTPConfig(
            server=row.get("smtp_server"),
            port=int(row.get("smtp_port") or 465),
            user=row.get("smtp_user"),
            has_password=bool(row.get("smtp_pass_encrypted")),
        ),
        recipients=recipients,
        thresholds={
            "invoice_increase_pct": _to_float(row.get("invoice_increase_pct")),
            "avg_cost_increase_pct": _to_float(row.get("avg_cost_increase_pct")),
            "avg_cost_vs_last_invoice_pct": _to_float(row.get("avg_cost_vs_last_invoice_pct")),
        },
        split_finished_goods=bool(row.get("split_finished_goods")),
    )


def _get_alert_settings_row(app_db: Session) -> dict:
    _ensure_alert_tables(app_db)
    row = app_db.execute(
        text(
            """
            SELECT *
            FROM alert_settings
            WHERE id = 1;
            """
        )
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=500, detail="Configuracao de alertas nao encontrada")

    return dict(row)


def _build_product_rule_alerts(
    app_db: Session,
    source_db: Session | None,
    product_code: str,
    current_settings: AlertSettingsResponse,
) -> list[dict]:
    thresholds = current_settings.thresholds
    invoice_increase_threshold = _to_float(thresholds.get("invoice_increase_pct"))
    avg_cost_increase_threshold = _to_float(thresholds.get("avg_cost_increase_pct"))
    avg_vs_invoice_threshold = _to_float(thresholds.get("avg_cost_vs_last_invoice_pct"))

    alerts: list[dict] = []

    latest_nf_row = app_db.execute(
        text(
            """
            WITH ranked AS (
                SELECT
                    produto_codigo,
                    nf_numero,
                    data_nf,
                    mp_por_peca,
                    loaded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY produto_codigo
                        ORDER BY data_nf DESC, loaded_at DESC, nf_numero DESC
                    ) AS rn
                FROM cost_map_nfs
                WHERE mp_por_peca IS NOT NULL
                  AND mp_por_peca > 0
                  AND produto_codigo = :code
            )
            SELECT
                l.produto_codigo,
                l.nf_numero AS latest_nf,
                l.data_nf AS latest_date,
                l.mp_por_peca AS latest_cost,
                p.nf_numero AS previous_nf,
                p.data_nf AS previous_date,
                p.mp_por_peca AS previous_cost,
                CASE
                    WHEN p.mp_por_peca > 0
                    THEN ((l.mp_por_peca - p.mp_por_peca) / p.mp_por_peca) * 100
                    ELSE 0
                END AS invoice_increase_pct
            FROM ranked l
            LEFT JOIN ranked p
                ON p.produto_codigo = l.produto_codigo
               AND p.rn = 2
            WHERE l.rn = 1;
            """
        ),
        {"code": product_code},
    ).mappings().first()

    if latest_nf_row:
        previous_cost = _to_float(latest_nf_row.get("previous_cost"))
        latest_cost = _to_float(latest_nf_row.get("latest_cost"))
        pct = _to_float(latest_nf_row.get("invoice_increase_pct"))
        if previous_cost > 0 and pct > invoice_increase_threshold:
            data_ref = _to_date(latest_nf_row.get("latest_date"))
            message = (
                f"Nota fiscal subiu {pct:.2f}% em relacao a anterior"
                if latest_nf_row.get("previous_nf")
                else f"Nota fiscal subiu {pct:.2f}%"
            )
            alerts.append(
                {
                    "product_code": product_code,
                    "tipo_alerta": "NF_ACIMA",
                    "descricao": None,
                    "custo_atual": latest_cost,
                    "variacao_percentual": pct,
                    "mensagem_alerta": message,
                    "severidade": "ALTA" if pct >= invoice_increase_threshold * 2 else "MEDIA",
                    "data_referencia": data_ref,
                }
            )

    avg_increase_row = app_db.execute(
        text(
            """
            WITH ranked_snapshot AS (
                SELECT
                    product_code,
                    custo_medio_real,
                    data_fechamento,
                    loaded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_code
                        ORDER BY data_fechamento DESC, loaded_at DESC
                    ) AS rn
                FROM product_cost_snapshot
                WHERE custo_medio_real IS NOT NULL
                  AND product_code = :code
            )
            SELECT
                cur.product_code,
                cur.custo_medio_real AS current_avg_cost,
                prev.custo_medio_real AS previous_avg_cost,
                cur.data_fechamento AS current_ref,
                prev.data_fechamento AS previous_ref,
                CASE
                    WHEN prev.custo_medio_real > 0
                    THEN ((cur.custo_medio_real - prev.custo_medio_real) / prev.custo_medio_real) * 100
                    ELSE 0
                END AS avg_cost_increase_pct
            FROM ranked_snapshot cur
            INNER JOIN ranked_snapshot prev
                ON prev.product_code = cur.product_code
               AND prev.rn = 2
            WHERE cur.rn = 1;
            """
        ),
        {"code": product_code},
    ).mappings().first()

    if avg_increase_row:
        pct = _to_float(avg_increase_row.get("avg_cost_increase_pct"))
        if pct > avg_cost_increase_threshold:
            data_ref = _to_date(avg_increase_row.get("current_ref"))
            alerts.append(
                {
                    "product_code": product_code,
                    "tipo_alerta": "CUSTO_MEDIO",
                    "descricao": None,
                    "custo_atual": _to_float(avg_increase_row.get("current_avg_cost")),
                    "variacao_percentual": pct,
                    "mensagem_alerta": f"Custo medio subiu {pct:.2f}% desde o ultimo fechamento",
                    "severidade": "ALTA" if pct >= avg_cost_increase_threshold * 2 else "MEDIA",
                    "data_referencia": data_ref,
                }
            )

    avg_vs_nf_row = app_db.execute(
        text(
            """
            SELECT
                product_code,
                descricao,
                ultimo_custo_nf,
                custo_medio_fech,
                data_ultima_nf,
                CASE
                    WHEN ultimo_custo_nf > 0
                    THEN ((custo_medio_fech - ultimo_custo_nf) / ultimo_custo_nf) * 100
                    ELSE 0
                END AS variance_pct
            FROM product_nf_cost_analysis
            WHERE product_code = :code
              AND ultimo_custo_nf IS NOT NULL
              AND ultimo_custo_nf > 0
              AND custo_medio_fech IS NOT NULL;
            """
        ),
        {"code": product_code},
    ).mappings().first()

    if avg_vs_nf_row:
        pct = _to_float(avg_vs_nf_row.get("variance_pct"))
        if abs(pct) > avg_vs_invoice_threshold:
            data_ref = _to_date(avg_vs_nf_row.get("data_ultima_nf"))
            alerts.append(
                {
                    "product_code": product_code,
                    "tipo_alerta": "CUSTO_VS_NF",
                    "descricao": avg_vs_nf_row.get("descricao"),
                    "custo_atual": _to_float(avg_vs_nf_row.get("custo_medio_fech")),
                    "variacao_percentual": pct,
                    "mensagem_alerta": f"Custo medio difere {pct:.2f}% da ultima NF",
                    "severidade": "ALTA" if abs(pct) >= avg_vs_invoice_threshold * 2 else "MEDIA",
                    "data_referencia": data_ref,
                }
            )

    if source_db is not None:
        try:
            connection = source_db.connection().connection
            cursor = connection.cursor()
            cursor.execute(
                BOM_COST_ROLLUP_SQL,
                (
                    product_code,
                    settings.bom_tipo_custo,
                    settings.bom_qtd_notas_media,
                    settings.bom_limite_variacao_preco,
                ),
            )

            rows = []
            while True:
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                if not cursor.nextset():
                    break
            cursor.close()

            bom_threshold = avg_vs_invoice_threshold
            ref_date = _to_date(settings.default_data_fechamento) if settings.default_data_fechamento else None
            ref_date = ref_date or date.today()

            for row in rows:
                status_alerta = (row.get("status_alerta") or "").strip()
                variacao = _to_float(row.get("variacao_preco"))
                if status_alerta == "SEM CUSTO":
                    descricao = f"{row.get('produto_codigo')} - {row.get('produto_descricao') or ''}".strip()
                    alerts.append(
                        {
                            "product_code": product_code,
                            "tipo_alerta": "BOM_SEM_CUSTO",
                            "descricao": descricao,
                            "custo_atual": None,
                            "variacao_percentual": None,
                            "mensagem_alerta": f"Componente sem custo: {descricao}",
                            "severidade": "MEDIA",
                            "data_referencia": ref_date,
                        }
                    )
                    continue

                if status_alerta.startswith("VAR ") and abs(variacao) > bom_threshold:
                    descricao = f"{row.get('produto_codigo')} - {row.get('produto_descricao') or ''}".strip()
                    alerts.append(
                        {
                            "product_code": product_code,
                            "tipo_alerta": "BOM_VARIACAO",
                            "descricao": descricao,
                            "custo_atual": _to_float(row.get("custo_unitario")) if row.get("custo_unitario") is not None else None,
                            "variacao_percentual": variacao,
                            "mensagem_alerta": f"Variacao no componente {descricao}: {variacao:.2f}%",
                            "severidade": "ALTA" if abs(variacao) >= bom_threshold * 2 else "MEDIA",
                            "data_referencia": ref_date,
                        }
                    )
        except Exception:
            logger.exception("Falha ao gerar alertas BOM para %s", product_code)

    return alerts


def _select_alert_reference_date(alerts: list[dict]) -> date | None:
    dates = [alert.get("data_referencia") for alert in alerts if alert.get("data_referencia")]
    if not dates:
        return None
    return max(dates)


def _get_latest_invoice_marker(app_db: Session) -> tuple[date | None, str | None]:
    row = app_db.execute(
        text(
            """
            SELECT data_nf, nf_numero
            FROM cost_map_nfs
            ORDER BY data_nf DESC, loaded_at DESC, nf_numero DESC
            LIMIT 1;
            """
        )
    ).mappings().first()

    if not row:
        return None, None
    return _to_date(row.get("data_nf")), row.get("nf_numero")


def _evaluate_alert_rules(app_db: Session, current_settings: AlertSettingsResponse) -> dict:
    thresholds = current_settings.thresholds
    invoice_increase_threshold = _to_float(thresholds.get("invoice_increase_pct"))
    avg_cost_increase_threshold = _to_float(thresholds.get("avg_cost_increase_pct"))
    avg_vs_invoice_threshold = _to_float(thresholds.get("avg_cost_vs_last_invoice_pct"))

    latest_nf_rows = app_db.execute(
        text(
            """
            WITH ranked AS (
                SELECT
                    produto_codigo,
                    nf_numero,
                    data_nf,
                    mp_por_peca,
                    loaded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY produto_codigo
                        ORDER BY data_nf DESC, loaded_at DESC, nf_numero DESC
                    ) AS rn
                FROM cost_map_nfs
                WHERE mp_por_peca IS NOT NULL
                  AND mp_por_peca > 0
            )
            SELECT
                l.produto_codigo,
                l.nf_numero AS latest_nf,
                l.data_nf AS latest_date,
                l.mp_por_peca AS latest_cost,
                p.nf_numero AS previous_nf,
                p.data_nf AS previous_date,
                p.mp_por_peca AS previous_cost,
                CASE
                    WHEN p.mp_por_peca > 0
                    THEN ((l.mp_por_peca - p.mp_por_peca) / p.mp_por_peca) * 100
                    ELSE 0
                END AS invoice_increase_pct
            FROM ranked l
            LEFT JOIN ranked p
                ON p.produto_codigo = l.produto_codigo
               AND p.rn = 2
            WHERE l.rn = 1;
            """
        )
    ).mappings().all()

    avg_vs_nf_rows = app_db.execute(
        text(
            """
            SELECT
                p.product_code,
                p.descricao,
                p.ultimo_custo_nf,
                p.custo_medio_fech,
                p.data_ultima_nf,
                CASE
                    WHEN p.ultimo_custo_nf > 0
                    THEN ((p.custo_medio_fech - p.ultimo_custo_nf) / p.ultimo_custo_nf) * 100
                    ELSE 0
                END AS variance_pct
            FROM product_nf_cost_analysis p
            WHERE p.ultimo_custo_nf IS NOT NULL
              AND p.ultimo_custo_nf > 0
              AND p.custo_medio_fech IS NOT NULL;
            """
        )
    ).mappings().all()

    descriptions_by_code = {
        str(row.get("product_code") or ""): str(row.get("descricao") or "")
        for row in avg_vs_nf_rows
    }

    latest_nf_date, latest_nf_number = _get_latest_invoice_marker(app_db)
    state_row = app_db.execute(
        text(
            """
            SELECT last_seen_nf_date, last_seen_nf_number
            FROM alert_rule_state
            WHERE rule_name = 'global_latest_nf';
            """
        )
    ).mappings().first()

    previous_seen_date = _to_date(state_row.get("last_seen_nf_date")) if state_row else None
    previous_seen_number = state_row.get("last_seen_nf_number") if state_row else None

    has_new_invoice = bool(latest_nf_date and latest_nf_number) and (
        previous_seen_date is None
        or latest_nf_date > previous_seen_date
        or (latest_nf_date == previous_seen_date and latest_nf_number != previous_seen_number)
    )

    rule_1_hits: list[RuleHitItem] = []
    for row in latest_nf_rows:
        previous_cost = _to_float(row.get("previous_cost"))
        latest_cost = _to_float(row.get("latest_cost"))
        pct = _to_float(row.get("invoice_increase_pct"))
        if previous_cost <= 0:
            continue
        if pct > invoice_increase_threshold:
            code = str(row.get("produto_codigo") or "")
            rule_1_hits.append(
                RuleHitItem(
                    product_code=code,
                    product_name=descriptions_by_code.get(code) or code,
                    variation_pct=round(pct, 2),
                    current_value=latest_cost,
                    reference_value=previous_cost,
                    latest_invoice_number=row.get("latest_nf"),
                    previous_invoice_number=row.get("previous_nf"),
                    is_finished_good=_is_finished_good(code),
                )
            )

    avg_increase_rows = app_db.execute(
        text(
            """
            WITH ranked_snapshot AS (
                SELECT
                    product_code,
                    custo_medio_real,
                    data_fechamento,
                    loaded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_code
                        ORDER BY data_fechamento DESC, loaded_at DESC
                    ) AS rn
                FROM product_cost_snapshot
                WHERE custo_medio_real IS NOT NULL
            )
            SELECT
                cur.product_code,
                cur.custo_medio_real AS current_avg_cost,
                prev.custo_medio_real AS previous_avg_cost,
                cur.data_fechamento AS current_ref,
                prev.data_fechamento AS previous_ref,
                CASE
                    WHEN prev.custo_medio_real > 0
                    THEN ((cur.custo_medio_real - prev.custo_medio_real) / prev.custo_medio_real) * 100
                    ELSE 0
                END AS avg_cost_increase_pct
            FROM ranked_snapshot cur
            INNER JOIN ranked_snapshot prev
                ON prev.product_code = cur.product_code
               AND prev.rn = 2
            WHERE cur.rn = 1;
            """
        )
    ).mappings().all()

    rule_2_hits: list[RuleHitItem] = []
    for row in avg_increase_rows:
        pct = _to_float(row.get("avg_cost_increase_pct"))
        if pct > avg_cost_increase_threshold:
            code = str(row.get("product_code") or "")
            rule_2_hits.append(
                RuleHitItem(
                    product_code=code,
                    product_name=descriptions_by_code.get(code) or code,
                    variation_pct=round(pct, 2),
                    current_value=_to_float(row.get("current_avg_cost")),
                    reference_value=_to_float(row.get("previous_avg_cost")),
                    is_finished_good=_is_finished_good(code),
                )
            )

    variance_items: list[CostVarianceItem] = []
    for row in avg_vs_nf_rows:
        pct = _to_float(row.get("variance_pct"))
        if abs(pct) > avg_vs_invoice_threshold:
            code = str(row.get("product_code") or "")
            variance_items.append(
                CostVarianceItem(
                    product_code=code,
                    product_name=str(row.get("descricao") or code),
                    last_invoice_cost=_to_float(row.get("ultimo_custo_nf")),
                    average_cost=_to_float(row.get("custo_medio_fech")),
                    variance_pct=round(pct, 2),
                    is_finished_good=_is_finished_good(code),
                )
            )

    if current_settings.split_finished_goods:
        variance_items.sort(
            key=lambda item: (
                0 if item.is_finished_good else 1,
                -abs(item.variance_pct),
                item.product_code,
            )
        )
    else:
        variance_items.sort(key=lambda item: (-abs(item.variance_pct), item.product_code))

    message = ""
    if not has_new_invoice:
        message = "Sem nota fiscal nova para comparar no momento."
    elif not variance_items:
        message = "Há nota fiscal nova, mas sem itens acima do limite configurado."

    return {
        "has_new_invoice": has_new_invoice,
        "items": variance_items if has_new_invoice else [],
        "message": message,
        "latest_marker": {
            "date": latest_nf_date.isoformat() if latest_nf_date else None,
            "number": latest_nf_number,
        },
        "rule_hits": {
            "invoice_vs_previous_invoice": [hit.model_dump() for hit in rule_1_hits],
            "avg_cost_increase_after_entry": [hit.model_dump() for hit in rule_2_hits],
        },
    }


@router.get("/alerts", response_model=AlertSettingsResponse)
async def get_alert_settings(app_db: Session = Depends(get_app_db)) -> AlertSettingsResponse:
    """Retorna configuracoes atuais de alertas para UI de configuracao."""
    row = _get_alert_settings_row(app_db)
    return _row_to_alert_settings(row)


@router.put("/alerts", response_model=AlertSettingsResponse)
async def update_alert_settings(
    payload: AlertSettingsUpdate,
    app_db: Session = Depends(get_app_db),
) -> AlertSettingsResponse:
    """Atualiza configuracoes de alertas com persistencia em banco."""
    try:
        before_row = _get_alert_settings_row(app_db)
        before_state = _row_to_alert_settings(before_row)

        smtp_server = payload.smtp.server if payload.smtp and payload.smtp.server is not None else before_row.get("smtp_server")
        smtp_port = payload.smtp.port if payload.smtp and payload.smtp.port is not None else before_row.get("smtp_port")
        smtp_user = payload.smtp.user if payload.smtp and payload.smtp.user is not None else before_row.get("smtp_user")
        smtp_pass_encrypted = before_row.get("smtp_pass_encrypted")

        if payload.smtp and payload.smtp.password is not None:
            password_value = payload.smtp.password.strip()
            smtp_pass_encrypted = _encrypt_secret(password_value) if password_value else None

        recipients = (
            payload.recipients
            if payload.recipients is not None
            else before_state.recipients
        )

        thresholds = before_state.thresholds.copy()
        if payload.thresholds is not None:
            thresholds = {
                "invoice_increase_pct": payload.thresholds.invoice_increase_pct,
                "avg_cost_increase_pct": payload.thresholds.avg_cost_increase_pct,
                "avg_cost_vs_last_invoice_pct": payload.thresholds.avg_cost_vs_last_invoice_pct,
            }

        split_finished_goods = (
            payload.split_finished_goods
            if payload.split_finished_goods is not None
            else before_state.split_finished_goods
        )

        app_db.execute(
            text(
                """
                UPDATE alert_settings
                SET
                    smtp_server = :smtp_server,
                    smtp_port = :smtp_port,
                    smtp_user = :smtp_user,
                    smtp_pass_encrypted = :smtp_pass_encrypted,
                    recipients_json = CAST(:recipients_json AS JSONB),
                    invoice_increase_pct = :invoice_increase_pct,
                    avg_cost_increase_pct = :avg_cost_increase_pct,
                    avg_cost_vs_last_invoice_pct = :avg_cost_vs_last_invoice_pct,
                    split_finished_goods = :split_finished_goods,
                    updated_at = NOW(),
                    updated_by = 'api'
                WHERE id = 1;
                """
            ),
            {
                "smtp_server": smtp_server,
                "smtp_port": smtp_port,
                "smtp_user": smtp_user,
                "smtp_pass_encrypted": smtp_pass_encrypted,
                "recipients_json": json.dumps([r.model_dump(exclude_none=True) for r in recipients]),
                "invoice_increase_pct": thresholds["invoice_increase_pct"],
                "avg_cost_increase_pct": thresholds["avg_cost_increase_pct"],
                "avg_cost_vs_last_invoice_pct": thresholds["avg_cost_vs_last_invoice_pct"],
                "split_finished_goods": split_finished_goods,
            },
        )

        after_row = _get_alert_settings_row(app_db)
        after_state = _row_to_alert_settings(after_row)

        app_db.execute(
            text(
                """
                INSERT INTO alert_settings_history (
                    changed_by,
                    action,
                    before_json,
                    after_json
                ) VALUES (
                    'api',
                    'update',
                    CAST(:before_json AS JSONB),
                    CAST(:after_json AS JSONB)
                );
                """
            ),
            {
                "before_json": json.dumps(before_state.model_dump()),
                "after_json": json.dumps(after_state.model_dump()),
            },
        )

        app_db.commit()
        return after_state
    except HTTPException:
        app_db.rollback()
        raise
    except InvalidToken as exc:
        app_db.rollback()
        raise HTTPException(status_code=500, detail="Falha na criptografia de credenciais") from exc
    except Exception as exc:
        app_db.rollback()
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar configuracao: {exc}") from exc
    finally:
        app_db.close()


@router.get("/alerts/preview", response_model=AlertPreviewResponse)
async def get_alert_preview(app_db: Session = Depends(get_app_db)) -> AlertPreviewResponse:
    """
    Preview de itens para o card de configuracoes.

    Engine de regras usando tabelas de cache no PostgreSQL.
    """
    row = _get_alert_settings_row(app_db)
    current_settings = _row_to_alert_settings(row)
    payload = _evaluate_alert_rules(app_db, current_settings)
    return AlertPreviewResponse(
        has_new_invoice=payload["has_new_invoice"],
        items=payload["items"],
        message=payload["message"],
        rule_hits=payload["rule_hits"],
        generated_at=datetime.now(timezone.utc),
    )


@router.get("/alerts/product/{product_code}", response_model=ProductAlertsResponse)
async def get_product_alerts(
    product_code: str,
    app_db: Session = Depends(get_app_db),
    source_db: Session = Depends(get_source_db),
) -> ProductAlertsResponse:
    settings_row = _get_alert_settings_row(app_db)
    current_settings = _row_to_alert_settings(settings_row)
    alerts_rows = _build_product_rule_alerts(app_db, source_db, product_code, current_settings)
    if not alerts_rows:
        return ProductAlertsResponse(product_code=product_code, data_referencia=None, alerts=[], pending_count=0)

    alert_keys = []
    for row in alerts_rows:
        alert_keys.append(
            _hash_alert_key(
                product_code=str(row.get("product_code") or product_code),
                tipo_alerta=str(row.get("tipo_alerta") or ""),
                mensagem_alerta=row.get("mensagem_alerta"),
                data_referencia=row.get("data_referencia"),
                custo_atual=_to_float(row.get("custo_atual")) if row.get("custo_atual") is not None else None,
                variacao_percentual=_to_float(row.get("variacao_percentual")) if row.get("variacao_percentual") is not None else None,
            )
        )

    sent_keys: set[str] = set()
    if alert_keys:
        existing = app_db.execute(
            text(
                """
                SELECT alert_key
                FROM alert_dispatch_log
                WHERE product_code = :code
                  AND alert_key IN :keys;
                """
            ).bindparams(bindparam("keys", expanding=True)),
            {"code": product_code, "keys": alert_keys},
        ).scalars().all()
        sent_keys = set(existing)

    alerts: list[ProductAlertItem] = []
    pending_count = 0
    for row, alert_key in zip(alerts_rows, alert_keys):
        sent = alert_key in sent_keys
        if not sent:
            pending_count += 1
        alerts.append(
            ProductAlertItem(
                product_code=str(row.get("product_code") or product_code),
                tipo_alerta=str(row.get("tipo_alerta") or ""),
                descricao=row.get("descricao"),
                custo_atual=_to_float(row.get("custo_atual")) if row.get("custo_atual") is not None else None,
                variacao_percentual=_to_float(row.get("variacao_percentual")) if row.get("variacao_percentual") is not None else None,
                mensagem_alerta=row.get("mensagem_alerta"),
                severidade=row.get("severidade"),
                data_referencia=datetime.combine(row.get("data_referencia"), time.min)
                if row.get("data_referencia")
                else None,
                sent=sent,
            )
        )

    data_referencia = _select_alert_reference_date(alerts_rows)
    return ProductAlertsResponse(
        product_code=product_code,
        data_referencia=data_referencia.isoformat() if data_referencia else None,
        alerts=alerts,
        pending_count=pending_count,
    )


@router.post("/alerts/product/{product_code}/dispatch", response_model=AlertDispatchResponse)
async def dispatch_product_alerts(
    product_code: str,
    app_db: Session = Depends(get_app_db),
    source_db: Session = Depends(get_source_db),
) -> AlertDispatchResponse:
    try:
        settings_row = _get_alert_settings_row(app_db)
        current_settings = _row_to_alert_settings(settings_row)

        smtp_server = current_settings.smtp.server
        smtp_port = int(current_settings.smtp.port)
        smtp_user = current_settings.smtp.user
        encrypted_pass = settings_row.get("smtp_pass_encrypted")
        recipients = [recipient.email for recipient in current_settings.recipients if recipient.email]

        if not smtp_server or not smtp_user:
            raise HTTPException(status_code=400, detail="Configure servidor e usuario SMTP antes do disparo")
        if not encrypted_pass:
            raise HTTPException(status_code=400, detail="Configure a senha SMTP antes do disparo")
        if not recipients:
            raise HTTPException(status_code=400, detail="Configure pelo menos um destinatario")

        alerts_rows = _build_product_rule_alerts(app_db, source_db, product_code, current_settings)
        if not alerts_rows:
            return AlertDispatchResponse(
                product_code=product_code,
                sent_count=0,
                skipped_count=0,
                failed_recipients=[],
                message="Sem alertas para este produto.",
            )

        alert_keys: list[str] = []
        for row in alerts_rows:
            alert_keys.append(
                _hash_alert_key(
                    product_code=str(row.get("product_code") or product_code),
                    tipo_alerta=str(row.get("tipo_alerta") or ""),
                    mensagem_alerta=row.get("mensagem_alerta"),
                    data_referencia=row.get("data_referencia"),
                    custo_atual=_to_float(row.get("custo_atual")) if row.get("custo_atual") is not None else None,
                    variacao_percentual=_to_float(row.get("variacao_percentual")) if row.get("variacao_percentual") is not None else None,
                )
            )

        existing = app_db.execute(
            text(
                """
                SELECT alert_key
                FROM alert_dispatch_log
                WHERE product_code = :code
                  AND alert_key IN :keys;
                """
            ).bindparams(bindparam("keys", expanding=True)),
            {"code": product_code, "keys": alert_keys},
        ).scalars().all()
        sent_keys = set(existing)

        pending_rows = []
        pending_keys = []
        for row, alert_key in zip(alerts_rows, alert_keys):
            if alert_key in sent_keys:
                continue
            pending_rows.append(row)
            pending_keys.append(alert_key)

        if not pending_rows:
            return AlertDispatchResponse(
                product_code=product_code,
                sent_count=0,
                skipped_count=len(alerts_rows),
                failed_recipients=[],
                message="Alertas ja enviados para este produto.",
            )

        data_referencia = _select_alert_reference_date(pending_rows)
        body_lines = [
            f"Produto: {product_code}",
            f"Data referencia: {data_referencia.isoformat() if data_referencia else '-'}",
            "",
            "Alertas:",
        ]
        for row in pending_rows:
            tipo = row.get("tipo_alerta") or "ALERTA"
            mensagem = row.get("mensagem_alerta") or ""
            custo_atual = row.get("custo_atual")
            variacao = row.get("variacao_percentual")
            detalhes = []
            if custo_atual is not None:
                detalhes.append(f"custo atual: {float(custo_atual):.2f}")
            if variacao is not None:
                detalhes.append(f"variacao: {float(variacao):.2f}%")
            detalhes_txt = f" ({', '.join(detalhes)})" if detalhes else ""
            body_lines.append(f"- [{tipo}] {mensagem}{detalhes_txt}")

        smtp_pass = _decrypt_secret(encrypted_pass)
        subject = f"[Steelbras] Alertas de custo - {product_code}"
        body = "\n".join(body_lines)

        if smtp_port == 465:
            connection = smtplib.SMTP_SSL(
                host=smtp_server,
                port=smtp_port,
                timeout=15,
                context=ssl.create_default_context(),
            )
        else:
            connection = smtplib.SMTP(host=smtp_server, port=smtp_port, timeout=15)
            connection.ehlo()
            connection.starttls(context=ssl.create_default_context())
            connection.ehlo()

        with connection:
            connection.login(smtp_user, smtp_pass)
            message = EmailMessage()
            message["From"] = smtp_user
            message["To"] = smtp_user
            message["Bcc"] = ", ".join(recipients)
            message["Subject"] = subject
            message.set_content(body)
            connection.send_message(message)

        for alert_key, row in zip(pending_keys, pending_rows):
            app_db.execute(
                text(
                    """
                    INSERT INTO alert_dispatch_log (
                        product_code,
                        alert_key,
                        alert_type,
                        data_referencia,
                        message
                    ) VALUES (
                        :product_code,
                        :alert_key,
                        :alert_type,
                        :data_referencia,
                        :message
                    )
                    ON CONFLICT (product_code, alert_key) DO NOTHING;
                    """
                ),
                {
                    "product_code": product_code,
                    "alert_key": alert_key,
                    "alert_type": row.get("tipo_alerta"),
                    "data_referencia": row.get("data_referencia"),
                    "message": row.get("mensagem_alerta"),
                },
            )

        app_db.commit()
        return AlertDispatchResponse(
            product_code=product_code,
            sent_count=len(pending_rows),
            skipped_count=len(alerts_rows) - len(pending_rows),
            failed_recipients=[],
            message="Alertas enviados com sucesso.",
        )
    except HTTPException:
        app_db.rollback()
        raise
    except InvalidToken as exc:
        app_db.rollback()
        raise HTTPException(status_code=500, detail="Senha SMTP invalida ou chave incorreta") from exc
    except Exception as exc:
        app_db.rollback()
        raise HTTPException(status_code=500, detail=f"Falha ao enviar alertas: {exc}") from exc


@router.post("/alerts/test-email")
async def send_test_email(app_db: Session = Depends(get_app_db)) -> dict:
    """
    Envia e-mail de teste para os destinatarios configurados.
    """
    status = "failed"
    success_count = 0
    fail_count = 0
    failed_recipients: list[str] = []
    error_message = None

    try:
        settings_row = _get_alert_settings_row(app_db)
        current_settings = _row_to_alert_settings(settings_row)

        smtp_server = current_settings.smtp.server
        smtp_port = int(current_settings.smtp.port)
        smtp_user = current_settings.smtp.user
        encrypted_pass = settings_row.get("smtp_pass_encrypted")
        recipients = [recipient.email for recipient in current_settings.recipients if recipient.email]

        if not smtp_server or not smtp_user:
            raise HTTPException(status_code=400, detail="Configure servidor e usuario SMTP antes do teste")
        if not encrypted_pass:
            raise HTTPException(status_code=400, detail="Configure a senha SMTP antes do teste")
        if not recipients:
            raise HTTPException(status_code=400, detail="Configure pelo menos um destinatario")

        smtp_pass = _decrypt_secret(encrypted_pass)
        smtp_timeout = 15

        if smtp_port == 465:
            connection = smtplib.SMTP_SSL(
                host=smtp_server,
                port=smtp_port,
                timeout=smtp_timeout,
                context=ssl.create_default_context(),
            )
        else:
            connection = smtplib.SMTP(host=smtp_server, port=smtp_port, timeout=smtp_timeout)
            connection.ehlo()
            connection.starttls(context=ssl.create_default_context())
            connection.ehlo()

        with connection:
            connection.login(smtp_user, smtp_pass)

            for recipient in recipients:
                message = EmailMessage()
                message["From"] = smtp_user
                message["To"] = recipient
                message["Subject"] = "[Steelbras] Teste de alerta SMTP"
                message.set_content(
                    "Este e um e-mail de teste dos alertas de custo.\n"
                    "Se voce recebeu esta mensagem, a configuracao SMTP esta funcional."
                )
                try:
                    connection.send_message(message)
                    success_count += 1
                except Exception:
                    fail_count += 1
                    failed_recipients.append(recipient)

        status = "success" if fail_count == 0 else "partial_success"

        if success_count > 0:
            latest_nf_date, latest_nf_number = _get_latest_invoice_marker(app_db)
            app_db.execute(
                text(
                    """
                    INSERT INTO alert_rule_state (
                        rule_name,
                        last_seen_nf_date,
                        last_seen_nf_number,
                        last_run_at
                    ) VALUES (
                        'global_latest_nf',
                        :last_seen_nf_date,
                        :last_seen_nf_number,
                        NOW()
                    )
                    ON CONFLICT (rule_name)
                    DO UPDATE SET
                        last_seen_nf_date = EXCLUDED.last_seen_nf_date,
                        last_seen_nf_number = EXCLUDED.last_seen_nf_number,
                        last_run_at = NOW();
                    """
                ),
                {
                    "last_seen_nf_date": latest_nf_date,
                    "last_seen_nf_number": latest_nf_number,
                },
            )

        app_db.execute(
            text(
                """
                INSERT INTO alert_email_runs (
                    requested_by,
                    status,
                    success_count,
                    fail_count,
                    failed_recipients,
                    error_message
                ) VALUES (
                    'api',
                    :status,
                    :success_count,
                    :fail_count,
                    CAST(:failed_recipients AS JSONB),
                    :error_message
                );
                """
            ),
            {
                "status": status,
                "success_count": success_count,
                "fail_count": fail_count,
                "failed_recipients": json.dumps(failed_recipients),
                "error_message": error_message,
            },
        )
        app_db.commit()

        return {
            "status": status,
            "message": "Teste de e-mail finalizado",
            "success_count": success_count,
            "fail_count": fail_count,
            "failed_recipients": failed_recipients,
        }
    except HTTPException as exc:
        app_db.rollback()
        raise exc
    except InvalidToken as exc:
        app_db.rollback()
        logger.exception("Falha ao descriptografar segredo SMTP")
        raise HTTPException(status_code=500, detail="Senha SMTP invalida ou chave incorreta") from exc
    except Exception as exc:
        app_db.rollback()
        status = "failed"
        error_message = str(exc)
        fail_count = max(fail_count, 1)
        try:
            app_db.execute(
                text(
                    """
                    INSERT INTO alert_email_runs (
                        requested_by,
                        status,
                        success_count,
                        fail_count,
                        failed_recipients,
                        error_message
                    ) VALUES (
                        'api',
                        :status,
                        :success_count,
                        :fail_count,
                        CAST(:failed_recipients AS JSONB),
                        :error_message
                    );
                    """
                ),
                {
                    "status": status,
                    "success_count": success_count,
                    "fail_count": fail_count,
                    "failed_recipients": json.dumps(failed_recipients),
                    "error_message": error_message,
                },
            )
            app_db.commit()
        except Exception:
            app_db.rollback()

        logger.exception("Erro no envio de e-mail de teste")
        raise HTTPException(status_code=500, detail=f"Falha no envio SMTP: {exc}") from exc
    finally:
        app_db.close()
