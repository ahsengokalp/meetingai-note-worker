"""Extraction-ready entrypoint for AI note generation."""

from meetingai_note_worker.services.meeting_note_service import analyze_meeting

__all__ = ["analyze_meeting"]
