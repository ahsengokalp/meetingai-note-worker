from __future__ import annotations

from email.message import EmailMessage
from email.utils import make_msgid
from html import escape
import logging
import smtplib
from datetime import datetime
from pathlib import Path
from typing import Any

from meetingai_shared.config import (
    MAIL_FROM,
    MAIL_TO_DIKKAN,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_TIMEOUT,
    SMTP_USER,
)
from meetingai_shared.repositories.meeting_store import MeetingStore


logger = logging.getLogger(__name__)
MAIL_LOGO_PATH = Path(__file__).resolve().parent / "assets" / "dikkan-logo.png"


def compact_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def load_brand_logo() -> tuple[bytes, str] | None:
    try:
        if MAIL_LOGO_PATH.is_file():
            suffix = MAIL_LOGO_PATH.suffix.lower().lstrip(".") or "png"
            return MAIL_LOGO_PATH.read_bytes(), suffix
    except OSError:
        return None
    return None


def parse_extra_recipients() -> list[dict[str, str]]:
    raw = MAIL_TO_DIKKAN.replace(";", ",")
    recipients: list[dict[str, str]] = []
    for item in raw.split(","):
        email = compact_text(item).lower()
        if email and all(existing["email"] != email for existing in recipients):
            recipients.append({"email": email, "name": ""})
    return recipients


def collect_meeting_recipients(
    store: MeetingStore,
    meeting_id: int,
    owner_username: str | None = None,
) -> list[dict[str, str]]:
    participants = store.list_meeting_participants(meeting_id, owner_username)
    recipients: list[dict[str, str]] = []

    for participant in participants:
        email = compact_text(participant.get("email")).lower()
        name = compact_text(
            participant.get("full_name")
            or f"{participant.get('first_name') or ''} {participant.get('last_name') or ''}"
        )
        if email and all(existing["email"] != email for existing in recipients):
            recipients.append({"email": email, "name": name})

    if recipients:
        return recipients

    for recipient in parse_extra_recipients():
        if all(existing["email"] != recipient["email"] for existing in recipients):
            recipients.append(recipient)

    return recipients


def build_mail_subject(meeting: dict[str, Any], note: dict[str, Any]) -> str:
    title = compact_text(note.get("title")) or compact_text(meeting.get("title")) or compact_text(meeting.get("name"))
    created_at = datetime.now().astimezone().strftime("%d.%m.%Y %H:%M")
    return f"{title or 'Toplanti'} AI Notu - {created_at}"


def build_mail_body(meeting: dict[str, Any], note: dict[str, Any]) -> str:
    title = compact_text(note.get("title")) or compact_text(meeting.get("title")) or compact_text(meeting.get("name"))
    summary = compact_text(note.get("summary"))
    context_and_objective = compact_text(note.get("context_and_objective"))
    main_topics = [compact_text(item) for item in note.get("main_topics") or [] if compact_text(item)]
    decisions = [compact_text(item) for item in note.get("decisions") or [] if compact_text(item)]
    decision_details = [item for item in (note.get("decision_details") or []) if isinstance(item, dict)]
    risks = [compact_text(item) for item in note.get("risks") or [] if compact_text(item)]
    open_questions = [compact_text(item) for item in note.get("open_questions") or [] if compact_text(item)]
    open_items = [item for item in (note.get("open_items") or []) if isinstance(item, dict)]
    tags = [compact_text(item) for item in note.get("tags") or [] if compact_text(item)]
    participant_contributions = [item for item in (note.get("participant_contributions") or []) if isinstance(item, dict)]
    action_items = note.get("action_items") or []

    lines: list[str] = [
        f"Toplanti: {title or 'Toplanti'}",
        "",
        "Ozet:",
        summary or "-",
    ]

    if context_and_objective:
        lines.extend(["", "Ana Amac & Baglam:", context_and_objective])

    if main_topics:
        lines.extend(["", "Gorusulen Ana Konular:"])
        lines.extend(f"- {item}" for item in main_topics)

    if participant_contributions:
        lines.extend(["", "Katilimci Katkilari:"])
        for participant in participant_contributions:
            name = compact_text(participant.get("name")) or "Katilimci"
            role = compact_text(participant.get("role"))
            contributions = [compact_text(item) for item in participant.get("contributions") or [] if compact_text(item)]
            lines.append(f"- {name}" + (f" ({role})" if role else ""))
            lines.extend(f"  * {item}" for item in contributions)

    if decision_details:
        lines.extend(["", "Kararlar:"])
        for item in decision_details:
            decision = compact_text(item.get("decision"))
            status = compact_text(item.get("status")) or "unknown"
            priority = compact_text(item.get("priority")) or "unknown"
            if decision:
                lines.append(f"- {decision} | Durum: {status} | Oncelik: {priority}")
    elif decisions:
        lines.extend(["", "Kararlar:"])
        lines.extend(f"- {item}" for item in decisions)

    if action_items:
        lines.extend(["", "Aksiyonlar:"])
        for action in action_items:
            if not isinstance(action, dict):
                continue
            task = compact_text(action.get("task"))
            owner = compact_text(action.get("owner")) or "unknown"
            due_date = compact_text(action.get("due_date")) or "unknown"
            status = compact_text(action.get("status")) or "unknown"
            priority = compact_text(action.get("priority")) or "unknown"
            if task:
                lines.append(
                    f"- {task} | Sorumlu: {owner} | Termin: {due_date} | Durum: {status} | Oncelik: {priority}"
                )

    if risks:
        lines.extend(["", "Riskler:"])
        lines.extend(f"- {item}" for item in risks)

    if open_items:
        lines.extend(["", "Acik Konular:"])
        for item in open_items:
            text = compact_text(item.get("item"))
            status = compact_text(item.get("status")) or "unknown"
            if text:
                lines.append(f"- {text} | Durum: {status}")
    elif open_questions:
        lines.extend(["", "Acik Sorular:"])
        lines.extend(f"- {item}" for item in open_questions)

    if tags:
        lines.extend(["", f"Etiketler: {', '.join(tags)}"])

    lines.extend(["", "Bu rapor AI destegiyle otomatik olusturulmustur."])

    return "\n".join(lines).strip() + "\n"


def render_html_list(items: list[str]) -> str:
    if not items:
        return "<p style=\"margin:0;color:#6b5a50;\">-</p>"
    return (
        "<ul style=\"margin:0;padding-left:18px;color:#2d241f;\">"
        + "".join(f"<li style=\"margin:0 0 8px;\">{escape(item)}</li>" for item in items)
        + "</ul>"
    )


def render_html_action_items(action_items: list[dict[str, Any]]) -> str:
    rows: list[str] = []
    for action in action_items:
        if not isinstance(action, dict):
            continue
        task = compact_text(action.get("task"))
        if not task:
            continue
        owner = compact_text(action.get("owner")) or "unknown"
        due_date = compact_text(action.get("due_date")) or "unknown"
        status = compact_text(action.get("status")) or "unknown"
        priority = compact_text(action.get("priority")) or "unknown"
        rows.append(
            "<tr>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(task)}</td>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(owner)}</td>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(due_date)}</td>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(status)}</td>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(priority)}</td>"
            "</tr>"
        )

    if not rows:
        return "<p style=\"margin:0;color:#6b5a50;\">-</p>"

    return (
        "<table role=\"presentation\" style=\"width:100%;border-collapse:collapse;background:#fffaf5;"
        "border:1px solid #f0dfd3;border-radius:12px;overflow:hidden;\">"
        "<thead>"
        "<tr style=\"background:#fff2e7;color:#7a3d1c;\">"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Aksiyon</th>"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Sorumlu</th>"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Termin</th>"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Durum</th>"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Oncelik</th>"
        "</tr>"
        "</thead>"
        "<tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def render_html_decision_details(items: list[dict[str, Any]]) -> str:
    rows: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        decision = compact_text(item.get("decision"))
        if not decision:
            continue
        status = compact_text(item.get("status")) or "unknown"
        priority = compact_text(item.get("priority")) or "unknown"
        rows.append(
            "<tr>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(decision)}</td>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(status)}</td>"
            f"<td style=\"padding:10px 12px;border-bottom:1px solid #f0dfd3;vertical-align:top;\">{escape(priority)}</td>"
            "</tr>"
        )

    if not rows:
        return "<p style=\"margin:0;color:#6b5a50;\">-</p>"

    return (
        "<table role=\"presentation\" style=\"width:100%;border-collapse:collapse;background:#fffaf5;"
        "border:1px solid #f0dfd3;border-radius:12px;overflow:hidden;\">"
        "<thead>"
        "<tr style=\"background:#fff2e7;color:#7a3d1c;\">"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Karar</th>"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Durum</th>"
        "<th style=\"padding:10px 12px;text-align:left;font-size:12px;\">Oncelik</th>"
        "</tr>"
        "</thead>"
        "<tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def render_html_open_items(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p style=\"margin:0;color:#6b5a50;\">-</p>"
    rows: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = compact_text(item.get("item"))
        if not text:
            continue
        status = compact_text(item.get("status")) or "unknown"
        rows.append(f"<li style=\"margin:0 0 8px;\">{escape(text)} <strong>({escape(status)})</strong></li>")
    if not rows:
        return "<p style=\"margin:0;color:#6b5a50;\">-</p>"
    return "<ul style=\"margin:0;padding-left:18px;color:#2d241f;\">" + "".join(rows) + "</ul>"


def render_html_participant_contributions(items: list[dict[str, Any]]) -> str:
    blocks: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = compact_text(item.get("name")) or "Katilimci"
        role = compact_text(item.get("role"))
        contributions = [compact_text(entry) for entry in item.get("contributions") or [] if compact_text(entry)]
        contribution_html = render_html_list(contributions)
        blocks.append(
            "<div style=\"margin-bottom:14px;padding:14px;background:#fff8f2;border:1px solid #f0dfd3;border-radius:14px;\">"
            f"<div style=\"font-weight:700;color:#2d241f;\">{escape(name)}</div>"
            + (f"<div style=\"margin-top:4px;font-size:13px;color:#6f4c39;\">{escape(role)}</div>" if role else "")
            + f"<div style=\"margin-top:10px;\">{contribution_html}</div>"
            "</div>"
        )
    if not blocks:
        return "<p style=\"margin:0;color:#6b5a50;\">-</p>"
    return "".join(blocks)


def build_mail_html(meeting: dict[str, Any], note: dict[str, Any], *, logo_cid: str | None = None) -> str:
    title = compact_text(note.get("title")) or compact_text(meeting.get("title")) or compact_text(meeting.get("name"))
    summary = compact_text(note.get("summary"))
    context_and_objective = compact_text(note.get("context_and_objective"))
    main_topics = [compact_text(item) for item in note.get("main_topics") or [] if compact_text(item)]
    decisions = [compact_text(item) for item in note.get("decisions") or [] if compact_text(item)]
    decision_details = [item for item in (note.get("decision_details") or []) if isinstance(item, dict)]
    risks = [compact_text(item) for item in note.get("risks") or [] if compact_text(item)]
    open_questions = [compact_text(item) for item in note.get("open_questions") or [] if compact_text(item)]
    open_items = [item for item in (note.get("open_items") or []) if isinstance(item, dict)]
    tags = [compact_text(item) for item in note.get("tags") or [] if compact_text(item)]
    participant_contributions = [item for item in (note.get("participant_contributions") or []) if isinstance(item, dict)]
    action_items = [item for item in (note.get("action_items") or []) if isinstance(item, dict)]
    created_at = datetime.now().astimezone().strftime("%d.%m.%Y %H:%M")
    meeting_name = escape(title or "Toplanti")
    logo_html = (
        f"<img src=\"cid:{escape(logo_cid)}\" alt=\"Dikkan\" "
        "style=\"display:block;width:176px;max-width:100%;height:auto;border:0;\">"
        if logo_cid
        else "<div style=\"font-size:34px;font-weight:800;letter-spacing:-0.04em;color:#f3761d;\">Dikkan</div>"
    )

    tag_html = (
        "".join(
            f"<span style=\"display:inline-block;margin:0 8px 8px 0;padding:6px 10px;border-radius:999px;"
            f"background:#fff1e6;color:#9a4b22;font-size:12px;font-weight:700;\">{escape(tag)}</span>"
            for tag in tags
        )
        if tags
        else "<p style=\"margin:0;color:#6b5a50;\">-</p>"
    )

    return f"""\
<!DOCTYPE html>
<html lang="tr">
  <body style="margin:0;padding:0;background-color:#fbf7f3;font-family:'Segoe UI',Arial,sans-serif;color:#2d241f;">
    <div style="padding:32px 16px;background:linear-gradient(180deg,#fffaf6 0%,#f5efe9 100%);">
      <div style="max-width:760px;margin:0 auto;background:#ffffff;border-radius:28px;overflow:hidden;border:1px solid #f0dfd3;box-shadow:0 18px 44px rgba(120,90,70,0.08);">
        <div style="height:8px;background:linear-gradient(90deg,#f3761d 0%,#f4a261 100%);"></div>
        <div style="padding:30px 32px 20px;background:linear-gradient(180deg,#fffaf6 0%,#fff4ec 100%);border-bottom:1px solid #f0dfd3;">
          <table role="presentation" style="width:100%;border-collapse:collapse;">
            <tr>
              <td style="vertical-align:top;padding:0 16px 14px 0;">
                {logo_html}
              </td>
              <td style="vertical-align:top;text-align:right;padding:0;">
                <div style="display:inline-block;padding:8px 12px;border-radius:999px;background:#fff1e6;color:#a85622;font-size:12px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;">Meeting Intelligence</div>
                <div style="margin-top:12px;font-size:13px;line-height:1.5;color:#7a6454;">AI Notu<br>{escape(created_at)}</div>
              </td>
            </tr>
          </table>
          <h1 style="margin:0 0 10px;font-size:28px;line-height:1.18;color:#2d241f;font-weight:800;letter-spacing:-0.04em;">{meeting_name}</h1>
          <p style="margin:0;font-size:15px;line-height:1.7;color:#6f4c39;">Toplanti ciktilari, aksiyonlar ve kritik kararlar bu raporda modernize edilmis bir ozet olarak yer alir.</p>
        </div>
        <div style="padding:26px 32px;">
          <div style="margin-bottom:20px;padding:20px 22px;background:#fff8f2;border:1px solid #f0dfd3;border-radius:18px;">
            <div style="font-size:12px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Yonetici Ozeti</div>
            <div style="font-size:15px;line-height:1.8;color:#2d241f;">{escape(summary or '-')}</div>
          </div>
          <div style="margin-bottom:18px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Ana Amac ve Baglam</div>
            <div style="font-size:15px;line-height:1.75;color:#2d241f;">{escape(context_and_objective or '-')}</div>
          </div>
          <div style="margin-bottom:18px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Gorusulen Ana Konular</div>
            {render_html_list(main_topics)}
          </div>
          <div style="margin-bottom:18px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Katilimci Katkilari</div>
            {render_html_participant_contributions(participant_contributions)}
          </div>
          <div style="margin-bottom:18px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Kararlar</div>
            {render_html_decision_details(decision_details) if decision_details else render_html_list(decisions)}
          </div>
          <div style="margin-bottom:18px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Aksiyonlar</div>
            {render_html_action_items(action_items)}
          </div>
          <div style="margin-bottom:18px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Riskler</div>
            {render_html_list(risks)}
          </div>
          <div style="margin-bottom:18px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Acik Konular</div>
            {render_html_open_items(open_items) if open_items else render_html_list(open_questions)}
          </div>
          <div style="margin-bottom:8px;padding:18px 20px;background:#ffffff;border:1px solid #f2e5da;border-radius:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#9a4b22;margin-bottom:10px;">Etiketler</div>
            {tag_html}
          </div>
          <div style="margin-top:18px;padding-top:18px;border-top:1px solid #ead9cc;font-size:12px;line-height:1.7;color:#6f4c39;">
            Bu rapor Dikkan Meeting Intelligence tarafindan AI destegiyle otomatik olusturulmustur.
          </div>
        </div>
      </div>
    </div>
  </body>
</html>
"""


def _delivery_summary(
    *,
    attempt_key: str,
    meeting_id: int,
    note_id: int | None,
    subject: str,
    recipients: list[dict[str, str]],
    trigger_source: str,
    requested_by: str | None,
    created_at: datetime,
    status: str,
    error_message: str = "",
) -> dict[str, Any]:
    recipient_emails = [recipient["email"] for recipient in recipients if compact_text(recipient.get("email"))]
    return {
        "attempt_key": attempt_key,
        "meeting_id": meeting_id,
        "note_id": note_id,
        "subject": subject,
        "trigger_source": trigger_source,
        "requested_by": compact_text(requested_by).casefold(),
        "created_at": created_at.isoformat(timespec="seconds"),
        "modified_at": created_at.strftime("%Y-%m-%d %H:%M"),
        "status": status,
        "recipient_count": len(recipient_emails),
        "sent_count": len(recipient_emails) if status == "sent" else 0,
        "failed_count": len(recipient_emails) if status == "failed" else 0,
        "error_message": compact_text(error_message),
        "recipients": recipient_emails,
    }


def _persist_delivery_summary(
    store: MeetingStore,
    *,
    meeting_id: int,
    note_id: int | None,
    subject: str,
    recipients: list[dict[str, str]],
    status: str,
    trigger_source: str,
    requested_by: str | None,
    owner_username: str | None,
    error_message: str,
    attempt_key: str,
    attempted_at: datetime,
) -> None:
    try:
        store.record_mail_delivery_attempt(
            meeting_id,
            note_id=note_id,
            subject=subject,
            recipients=recipients,
            status=status,
            trigger_source=trigger_source,
            requested_by=requested_by,
            owner_username=owner_username,
            error_message=error_message,
            attempt_key=attempt_key,
            attempted_at=attempted_at,
        )
    except Exception as exc:
        logger.warning("Mail delivery audit log could not be persisted for meeting %s: %s", meeting_id, exc)


def send_meeting_note_email(
    store: MeetingStore,
    meeting_id: int,
    note: dict[str, Any],
    *,
    note_id: int | None = None,
    owner_username: str | None = None,
    trigger_source: str = "analyze",
    requested_by: str | None = None,
) -> dict[str, Any]:
    attempt_time = datetime.now().astimezone()

    meeting = store.get_meeting(meeting_id, owner_username)
    if meeting is None:
        raise ValueError(f"Meeting not found: {meeting_id}")

    recipients = collect_meeting_recipients(store, meeting_id, owner_username)
    if not recipients:
        return _delivery_summary(
            attempt_key="",
            meeting_id=meeting_id,
            note_id=note_id,
            subject=build_mail_subject(meeting, note),
            recipients=recipients,
            trigger_source=trigger_source,
            requested_by=requested_by or owner_username,
            created_at=attempt_time,
            status="skipped",
        )

    subject = build_mail_subject(meeting, note)
    attempt_key = datetime.now().astimezone().strftime("%Y%m%d%H%M%S%f")

    if not SMTP_HOST:
        error_message = "SMTP host is not configured."
        _persist_delivery_summary(
            store,
            meeting_id=meeting_id,
            note_id=note_id,
            subject=subject,
            recipients=recipients,
            status="failed",
            trigger_source=trigger_source,
            requested_by=requested_by or owner_username,
            owner_username=owner_username,
            error_message=error_message,
            attempt_key=attempt_key,
            attempted_at=attempt_time,
        )
        return _delivery_summary(
            attempt_key=attempt_key,
            meeting_id=meeting_id,
            note_id=note_id,
            subject=subject,
            recipients=recipients,
            trigger_source=trigger_source,
            requested_by=requested_by or owner_username,
            created_at=attempt_time,
            status="failed",
            error_message=error_message,
        )

    logo_meta = load_brand_logo()
    logo_cid = make_msgid(domain="dikkan.local")[1:-1] if logo_meta else None

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = MAIL_FROM or "meeting-intelligence@localhost"
    message["To"] = ", ".join(recipient["email"] for recipient in recipients)
    message.set_content(build_mail_body(meeting, note))
    message.add_alternative(build_mail_html(meeting, note, logo_cid=logo_cid), subtype="html")
    if logo_meta:
        logo_bytes, logo_subtype = logo_meta
        try:
            html_part = message.get_payload()[-1]
            html_part.add_related(
                logo_bytes,
                maintype="image",
                subtype=logo_subtype,
                cid=f"<{logo_cid}>",
                disposition="inline",
                filename=f"dikkan-logo.{logo_subtype}",
            )
        except Exception as exc:
            logger.warning("Mail logo could not be embedded inline: %s", exc)

    try:
        with smtplib.SMTP(host=SMTP_HOST, port=SMTP_PORT, timeout=max(int(SMTP_TIMEOUT), 1)) as smtp:
            if SMTP_USER:
                smtp.login(SMTP_USER, SMTP_PASSWORD)
            smtp.send_message(message)
    except Exception as exc:
        error_message = str(exc)
        _persist_delivery_summary(
            store,
            meeting_id=meeting_id,
            note_id=note_id,
            subject=subject,
            recipients=recipients,
            status="failed",
            trigger_source=trigger_source,
            requested_by=requested_by or owner_username,
            owner_username=owner_username,
            error_message=error_message,
            attempt_key=attempt_key,
            attempted_at=attempt_time,
        )
        return _delivery_summary(
            attempt_key=attempt_key,
            meeting_id=meeting_id,
            note_id=note_id,
            subject=subject,
            recipients=recipients,
            trigger_source=trigger_source,
            requested_by=requested_by or owner_username,
            created_at=attempt_time,
            status="failed",
            error_message=error_message,
        )

    _persist_delivery_summary(
        store,
        meeting_id=meeting_id,
        note_id=note_id,
        subject=subject,
        recipients=recipients,
        status="sent",
        trigger_source=trigger_source,
        requested_by=requested_by or owner_username,
        owner_username=owner_username,
        error_message="",
        attempt_key=attempt_key,
        attempted_at=attempt_time,
    )
    logger.info("Meeting note email sent to %s recipient(s) for meeting %s", len(recipients), meeting_id)
    return _delivery_summary(
        attempt_key=attempt_key,
        meeting_id=meeting_id,
        note_id=note_id,
        subject=subject,
        recipients=recipients,
        trigger_source=trigger_source,
        requested_by=requested_by or owner_username,
        created_at=attempt_time,
        status="sent",
    )


def resend_meeting_note_email(
    store: MeetingStore,
    note_id: int,
    *,
    owner_username: str | None = None,
    requested_by: str | None = None,
) -> dict[str, Any]:
    note = store.get_note(note_id, owner_username)
    if note is None:
        raise FileNotFoundError(f"Meeting note not found: {note_id}")

    meeting_id = int(note["meeting_id"])
    return send_meeting_note_email(
        store,
        meeting_id,
        note,
        note_id=note_id,
        owner_username=owner_username,
        trigger_source="resend",
        requested_by=requested_by or owner_username,
    )
