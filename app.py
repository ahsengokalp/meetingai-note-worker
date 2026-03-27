from __future__ import annotations

from flask import Flask, jsonify, request

from meetingai_note_worker.services.meeting_mail_service import resend_meeting_note_email, send_meeting_note_email
from meetingai_note_worker.services.meeting_note_service import analyze_meeting
from meetingai_shared.config import WORKER_INTERNAL_TOKEN
from meetingai_shared.repositories.meeting_store import MeetingStore, meeting_can_generate_note


store = MeetingStore()


def json_error(message: str, status: int = 400):
    return jsonify({"error": message or "Request failed."}), status


def worker_exception_status(exc: Exception) -> int:
    if isinstance(exc, PermissionError):
        return 403
    if isinstance(exc, FileNotFoundError):
        return 404
    if isinstance(exc, ValueError):
        return 404 if "not found" in str(exc).lower() else 400
    if isinstance(exc, RuntimeError):
        return 400
    return 500


def _verify_internal_request() -> None:
    if not WORKER_INTERNAL_TOKEN:
        return
    presented = str(request.headers.get("X-Worker-Token") or "").strip()
    if presented != WORKER_INTERNAL_TOKEN:
        raise PermissionError("Invalid worker token.")


def create_app() -> Flask:
    app = Flask(__name__)

    @app.before_request
    def _guard_internal_token():
        if request.path == "/health":
            return None
        _verify_internal_request()
        return None

    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    @app.post("/internal/meetings/<int:meeting_id>/analyze")
    def analyze_meeting_endpoint(meeting_id: int):
        try:
            data = request.get_json(silent=True) or {}
            owner_username = str(data.get("owner_username") or "").strip() or None
            requested_by = str(data.get("requested_by") or "").strip() or owner_username
            title = str(data.get("title") or "").strip() or None
            trigger_source = str(data.get("trigger_source") or "analyze").strip() or "analyze"

            meeting = store.get_meeting(meeting_id, owner_username)
            if meeting is None:
                return json_error("Meeting not found.", 404)
            if not meeting_can_generate_note(meeting):
                return json_error("Final transcript is not ready yet. Wait for large-v3 transcription to finish.", 409)
            if title:
                store.update_meeting_state(meeting_id, title=title, owner_username=owner_username)

            note_id, note_data = analyze_meeting(store, meeting_id, title, owner_username)
            mail_result = send_meeting_note_email(
                store,
                meeting_id,
                note_data,
                note_id=note_id,
                owner_username=owner_username,
                trigger_source=trigger_source,
                requested_by=requested_by,
            )
            note = store.get_note(note_id, owner_username)
            if note is None:
                return json_error("Meeting note could not be loaded.", 500)
            response = dict(note)
            response["mail_recipient_count"] = int(mail_result["recipient_count"] or 0)
            response["mail_recipients"] = mail_result["recipients"]
            response["mail_error"] = mail_result["error_message"] or None
            response["mail_status"] = mail_result["status"]
            response["mail_sent_count"] = int(mail_result["sent_count"] or 0)
            response["mail_failed_count"] = int(mail_result["failed_count"] or 0)
            return jsonify(response), 201
        except Exception as exc:
            return json_error(str(exc), worker_exception_status(exc))

    @app.post("/internal/notes/<int:note_id>/send-mail")
    def resend_mail_endpoint(note_id: int):
        try:
            data = request.get_json(silent=True) or {}
            owner_username = str(data.get("owner_username") or "").strip() or None
            requested_by = str(data.get("requested_by") or "").strip() or owner_username
            result = resend_meeting_note_email(
                store,
                note_id,
                owner_username=owner_username,
                requested_by=requested_by,
            )
            return jsonify(result)
        except Exception as exc:
            return json_error(str(exc), worker_exception_status(exc))

    return app


__all__ = ["create_app", "store"]
