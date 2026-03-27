from __future__ import annotations

from datetime import datetime
import json
import logging
from pathlib import Path
import re
from typing import Any

from meetingai_note_worker.services.ollama_service import OllamaService
from meetingai_shared.config import MEETINGAI_NOTES_DIR, OLLAMA_BASE_URL, OLLAMA_MODEL, OLLAMA_TIMEOUT
from meetingai_shared.contracts.note_schema import NOTE_JSON_SCHEMA
from meetingai_shared.repositories.meeting_store import MeetingStore, normalize_note_payload


logger = logging.getLogger(__name__)

MAP_REDUCE_TRANSCRIPT_CHAR_THRESHOLD = 14_000
MAP_REDUCE_CHUNK_TARGET_CHARS = 9_000
MAP_REDUCE_CHUNK_OVERLAP_CHARS = 1_200


SYSTEM = (
    "You are a meeting assistant for DIKKAN Group. "
    "Write in Turkish. Return JSON only. No markdown. "
    "No explanations. Be factual, thorough, detail-rich, and operationally useful. "
    "Prefer completeness over brevity when the transcript supports it."
)

PROMPT_TEMPLATE = """\
Analyze the meeting transcripts below and return output that follows this JSON schema exactly:

{{
  "title": "string",
  "summary": "string",
  "context_and_objective": "string",
  "main_topics": ["string"],
  "participant_contributions": [
    {{
      "name": "string",
      "role": "string",
      "contributions": ["string"]
    }}
  ],
  "decisions": ["string"],
  "decision_details": [
    {{
      "decision": "string",
      "status": "string",
      "priority": "P0|P1|P2|P3|high|medium|low|unknown"
    }}
  ],
  "action_items": [
    {{
      "task": "string",
      "owner": "string|unknown",
      "due_date": "YYYY-MM-DD|unknown",
      "status": "string",
      "priority": "P0|P1|P2|P3|high|medium|low|unknown"
    }}
  ],
  "risks": ["string"],
  "open_questions": ["string"],
  "open_items": [
    {{
      "item": "string",
      "status": "string"
    }}
  ],
  "tags": ["dokumhane|talasli_imalat|montaj|test|kalite|arge|tasarim|marine|endustriyel|lojistik|satinalma|bakim|planlama|uretim|genel"]
}}

Company and factory context:
DIKKAN is the main company group context for these meetings.
Relevant group companies may include Dikkan Gemi, Dikkan Dis, Dikkan Metal, Iz-Metal, Izmet, Dikkan Kablo, Jonas GmbH, DK Technic Yapi, and Aliaga.
Dikkan Group has an integrated manufacturing structure covering the full process from raw material to finished product.
Main factory units include foundry, machining, assembly, and testing.
The factory designs and mass-produces bronze, cast iron, and steel alloy valves for marine and industrial sectors.
Production starts with melting metal alloys in modern foundry furnaces, continues with precision CNC machining, then assembly, and finally high-pressure leak-tightness testing.
Products are expected to comply with international quality standards.
The R&D and design offices also work on digital production tracking, efficiency analysis, and operational modernization.

Company-specific terminology:
- "FMS", "efemes", or similar phonetic references may refer to Fatih Mehmet Sancak.
- Fatih Mehmet Sancak is the CEO of Dikkan Group and Chairman of the Executive Board.
- Use this information only when it helps interpret the transcript more accurately.

Meeting context/title:
{context_title}

Meeting participants:
{participant_context}

Instructions:
- Use the final transcript as the primary source whenever it is available.
- Use the live transcript only as supporting context.
- If the final transcript and live transcript conflict, prefer the final transcript.
- Use the live transcript only to recover details that may be missing or unclear in the final transcript.
- Use company and factory context only to interpret domain-specific terms, company names, and abbreviations.
- Use participant names and job titles only as supporting context.
- If a person is referenced by first name, surname, or role, you may use the participant list to resolve identity.
- If company abbreviations, phonetic spellings, or short references appear, you may use the company-specific terminology section to interpret them.
- If the transcript contains obvious transcription noise, repeated fragments, filler words, or incomplete meaningless phrases, ignore those parts.
- If the transcript is ambiguous or fragmented, do not turn weak evidence into a decision or action item.
- Only extract decisions and action items that are explicitly stated or clearly supported by the transcript.
- Do not invent facts from company context, factory context, or participant context alone.
- Do not assign task ownership based on role alone unless the transcript clearly supports it.
- If ownership is unclear, use "unknown".
- If due date is unclear, use "unknown".
- Write a long-form meeting note, not a short abstract.
- Default to a note that is approximately 3x to 4x richer than a standard short meeting summary when the transcript has enough content.
- The summary must be self-sufficient, operationally useful, and materially detailed. Make it long, dense, and specific rather than brief.
- Prefer at least 8 to 16 full sentences in the summary when the transcript is rich enough. Do not compress the meeting into a few generic lines.
- Use context_and_objective to explain why the meeting happened, what problem space it covered, what background constraints mattered, and what business or operational context framed the discussion.
- Make context_and_objective substantially detailed as well; prefer at least 4 to 8 full sentences when the transcript supports it.
- Populate main_topics with detailed topic summaries. Each item should explain the issue, what was discussed, what alternatives or blockers appeared, and why the topic mattered operationally.
- Prefer more topic coverage instead of fewer broad bullets. When the transcript is rich, aim for 6 to 12 detailed main_topics items instead of 2 or 3 short ones.
- Populate participant_contributions for people who made meaningful contributions. Summarize each person's concrete inputs, arguments, concerns, clarifications, and commitments rather than generic praise.
- When participant_contributions are present, prefer multiple contribution entries per person if the transcript supports them.
- Populate decision_details with status and priority whenever the transcript supports them. If unclear, use "unknown".
- Populate decisions, decision_details, action_items, risks, open_questions, and open_items with full-sentence descriptions, not short fragments.
- For action_items, explain what needs to be done and include surrounding context if it helps execution.
- For risks and open_questions, explain why each item matters, what could happen, or what is still unknown.
- Populate open_items for unresolved issues, pending clarifications, dependencies, and items that still need follow-up.
- Prefer tags that best match the actual meeting content.
- If the meeting is mainly about production, quality, testing, maintenance, planning, shipment, or operational delays, reflect that clearly in the summary, decisions, risks, and tags.
- Avoid generic filler such as "meeting discussed various topics" or "important points were evaluated" unless you immediately specify what those topics or points were.
- Every section should preserve concrete names, departments, blockers, production stages, quality concerns, delivery timing, and technical references when the transcript supports them.

Final transcript (primary source, higher priority):
\"\"\"{final_transcript}\"\"\"

Live transcript (secondary source, lower priority):
\"\"\"{live_transcript}\"\"\"
"""

CHUNK_EXTRACTION_PROMPT_TEMPLATE = """\
Analyze only the transcript chunk below and extract structured meeting information.
Return JSON only and follow the response schema exactly.

Meeting context/title:
{context_title}

Meeting participants:
{participant_context}

Rules:
- This is only one chunk, not the whole meeting.
- Use only information that is explicitly stated or clearly supported by this chunk.
- Do not invent missing context from other parts of the meeting.
- If ownership or due date is unclear, use "unknown".
- Keep the chunk summary concise but concrete. Aim for 4 to 8 factual sentences.
- Extract detailed main_topics, decisions, action_items, risks, open_questions, and open_items whenever the chunk supports them.
- Use participant_contributions only when a person's contribution is actually visible in this chunk.
- Prefer empty arrays over weak guesses.

Chunk label:
{chunk_label}

Transcript chunk:
\"\"\"{chunk_text}\"\"\"
"""

FINAL_NOTE_FROM_CHUNKS_PROMPT_TEMPLATE = """\
Create one final long-form meeting note from the structured chunk materials below.
Return JSON only and follow the response schema exactly.

Meeting context/title:
{context_title}

Meeting participants:
{participant_context}

Rules:
- Use only the information contained in the structured chunk materials below.
- Merge duplicate or overlapping items.
- The final summary and context_and_objective must be long, detailed, and operationally useful.
- Preserve concrete people, departments, blockers, technical details, production stages, quality issues, and timing references when they appear in the extracted material.
- Do not invent facts that do not appear in the materials below.
- If ownership or due date is unclear, use "unknown".
- Keep decisions, action_items, risks, open_questions, and open_items concrete and explicit.

Structured chunk materials:
{structured_materials}
"""


def build_fallback_summary(transcript: str, limit: int = 1400) -> str:
    compact = " ".join(str(transcript or "").split()).strip()
    if not compact:
        return ""
    if len(compact) <= limit:
        return compact
    truncated = compact[:limit].rsplit(" ", 1)[0].strip()
    return f"{truncated or compact[:limit].strip()}..."


def compact_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def has_meaningful_note_content(note: dict[str, Any]) -> bool:
    return any(
        [
            compact_text(note.get("summary")),
            compact_text(note.get("context_and_objective")),
            note.get("main_topics"),
            note.get("participant_contributions"),
            note.get("decisions"),
            note.get("decision_details"),
            note.get("action_items"),
            note.get("risks"),
            note.get("open_questions"),
            note.get("open_items"),
        ]
    )


def build_participant_context(participants: list[dict[str, Any]] | None = None) -> str:
    normalized_participants = participants or []
    if not normalized_participants:
        return "No participant context provided."

    lines: list[str] = []
    for index, participant in enumerate(normalized_participants, start=1):
        full_name = compact_text(participant.get("full_name"))
        if not full_name:
            full_name = compact_text(
                f"{participant.get('first_name') or ''} {participant.get('last_name') or ''}"
            )
        email = compact_text(participant.get("email"))
        job_title = compact_text(participant.get("job_title"))
        label = full_name or email or f"Participant {index}"
        details = [item for item in (job_title, email) if item]
        lines.append(f"- {label}" + (f" | {' | '.join(details)}" if details else ""))

    return "\n".join(lines)


def normalize_key(value: Any) -> str:
    return re.sub(r"\W+", " ", compact_text(value).casefold()).strip()


def dedupe_text_items(values: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = compact_text(value)
        if not text:
            continue
        key = normalize_key(text)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def merge_participant_contributions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = compact_text(item.get("name")) or "unknown"
        key = normalize_key(name)
        if not key:
            continue
        if key not in merged:
            merged[key] = {"name": name, "role": compact_text(item.get("role")), "contributions": []}
            order.append(key)
        elif not compact_text(merged[key].get("role")):
            merged[key]["role"] = compact_text(item.get("role"))

        existing_keys = {normalize_key(value) for value in merged[key]["contributions"]}
        for contribution in item.get("contributions") or []:
            text = compact_text(contribution)
            contribution_key = normalize_key(text)
            if not text or not contribution_key or contribution_key in existing_keys:
                continue
            merged[key]["contributions"].append(text)
            existing_keys.add(contribution_key)

    return [merged[key] for key in order]


def prefer_non_unknown(current: str, candidate: str) -> str:
    current_text = compact_text(current) or "unknown"
    candidate_text = compact_text(candidate) or "unknown"
    if current_text == "unknown" and candidate_text != "unknown":
        return candidate_text
    return current_text


def merge_decision_details(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        decision = compact_text(item.get("decision"))
        if not decision:
            continue
        key = normalize_key(decision)
        if key not in merged:
            merged[key] = {
                "decision": decision,
                "status": compact_text(item.get("status")) or "unknown",
                "priority": compact_text(item.get("priority")) or "unknown",
            }
            order.append(key)
            continue
        merged[key]["status"] = prefer_non_unknown(merged[key]["status"], str(item.get("status") or "unknown"))
        merged[key]["priority"] = prefer_non_unknown(
            merged[key]["priority"],
            str(item.get("priority") or "unknown"),
        )
    return [merged[key] for key in order]


def merge_action_items(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        task = compact_text(item.get("task"))
        if not task:
            continue
        owner = compact_text(item.get("owner")) or "unknown"
        key = normalize_key(f"{task}::{owner}")
        if key not in merged:
            merged[key] = {
                "task": task,
                "owner": owner,
                "due_date": compact_text(item.get("due_date")) or "unknown",
                "status": compact_text(item.get("status")) or "unknown",
                "priority": compact_text(item.get("priority")) or "unknown",
            }
            order.append(key)
            continue
        merged[key]["due_date"] = prefer_non_unknown(merged[key]["due_date"], str(item.get("due_date") or "unknown"))
        merged[key]["status"] = prefer_non_unknown(merged[key]["status"], str(item.get("status") or "unknown"))
        merged[key]["priority"] = prefer_non_unknown(
            merged[key]["priority"],
            str(item.get("priority") or "unknown"),
        )
    return [merged[key] for key in order]


def merge_open_items(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = compact_text(item.get("item"))
        if not text:
            continue
        key = normalize_key(text)
        if key not in merged:
            merged[key] = {"item": text, "status": compact_text(item.get("status")) or "unknown"}
            order.append(key)
            continue
        merged[key]["status"] = prefer_non_unknown(merged[key]["status"], str(item.get("status") or "unknown"))
    return [merged[key] for key in order]


def split_transcript_into_units(transcript: str) -> list[str]:
    lines = [compact_text(line) for line in transcript.splitlines() if compact_text(line)]
    if len(lines) >= 6:
        return lines

    sentences = [compact_text(part) for part in re.split(r"(?<=[.!?])\s+", transcript) if compact_text(part)]
    if len(sentences) >= 4:
        return sentences

    words = transcript.split()
    if not words:
        return []
    return [" ".join(words[index : index + 120]).strip() for index in range(0, len(words), 120)]


def split_transcript_into_chunks(
    transcript: str,
    target_chars: int = MAP_REDUCE_CHUNK_TARGET_CHARS,
    overlap_chars: int = MAP_REDUCE_CHUNK_OVERLAP_CHARS,
) -> list[str]:
    units = split_transcript_into_units(transcript)
    if not units:
        return []

    chunks: list[str] = []
    current_units: list[str] = []
    current_chars = 0

    for unit in units:
        unit_chars = len(unit) + 1
        if current_units and current_chars + unit_chars > target_chars:
            chunks.append(" ".join(current_units).strip())
            overlap_units: list[str] = []
            overlap_used = 0
            for existing in reversed(current_units):
                overlap_units.insert(0, existing)
                overlap_used += len(existing) + 1
                if overlap_used >= overlap_chars:
                    break
            current_units = overlap_units[:]
            current_chars = sum(len(item) + 1 for item in current_units)

        current_units.append(unit)
        current_chars += unit_chars

    if current_units:
        chunks.append(" ".join(current_units).strip())

    return [chunk for chunk in chunks if chunk]


def build_empty_note_payload(title: str | None = None, summary: str = "") -> dict[str, Any]:
    return {
        "title": compact_text(title),
        "summary": compact_text(summary),
        "context_and_objective": "",
        "main_topics": [],
        "participant_contributions": [],
        "decisions": [],
        "decision_details": [],
        "action_items": [],
        "risks": [],
        "open_questions": [],
        "open_items": [],
        "tags": [],
    }


def merge_chunk_note_payloads(chunk_notes: list[dict[str, Any]], title: str | None = None) -> dict[str, Any]:
    merged = normalize_note_payload(
        {
            "title": compact_text(title) or compact_text(next((item.get("title") for item in chunk_notes if compact_text(item.get("title"))), "")),
            "summary": " ".join(dedupe_text_items([item.get("summary") for item in chunk_notes])),
            "context_and_objective": " ".join(
                dedupe_text_items([item.get("context_and_objective") for item in chunk_notes])
            ),
            "main_topics": dedupe_text_items(
                [topic for item in chunk_notes for topic in (item.get("main_topics") or [])]
            ),
            "participant_contributions": merge_participant_contributions(
                [participant for item in chunk_notes for participant in (item.get("participant_contributions") or [])]
            ),
            "decisions": dedupe_text_items(
                [decision for item in chunk_notes for decision in (item.get("decisions") or [])]
            ),
            "decision_details": merge_decision_details(
                [detail for item in chunk_notes for detail in (item.get("decision_details") or [])]
            ),
            "action_items": merge_action_items(
                [action for item in chunk_notes for action in (item.get("action_items") or [])]
            ),
            "risks": dedupe_text_items([risk for item in chunk_notes for risk in (item.get("risks") or [])]),
            "open_questions": dedupe_text_items(
                [question for item in chunk_notes for question in (item.get("open_questions") or [])]
            ),
            "open_items": merge_open_items(
                [open_item for item in chunk_notes for open_item in (item.get("open_items") or [])]
            ),
            "tags": dedupe_text_items([tag for item in chunk_notes for tag in (item.get("tags") or [])]),
        }
    )
    if not compact_text(merged.get("title")):
        merged["title"] = compact_text(title) or "Toplanti notu"
    return merged


def should_use_map_reduce(transcript: str) -> bool:
    return len(compact_text(transcript)) >= MAP_REDUCE_TRANSCRIPT_CHAR_THRESHOLD


def analyze_transcript_text_single_pass(
    transcript: str,
    supporting_transcript: str | None = None,
    title: str | None = None,
    participants: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cleaned = transcript.strip()
    if not cleaned:
        raise ValueError("Transcript looks empty.")
    cleaned_supporting = str(supporting_transcript or "").strip()
    participant_count = len(participants or [])

    ollama = OllamaService(OLLAMA_BASE_URL, OLLAMA_MODEL, timeout=OLLAMA_TIMEOUT)
    context_title = str(title or "").strip() or "No extra context provided."
    participant_context = build_participant_context(participants)
    prompt = PROMPT_TEMPLATE.format(
        final_transcript=cleaned,
        live_transcript=cleaned_supporting or "No secondary transcript provided.",
        context_title=context_title,
        participant_context=participant_context,
    )
    logger.info(
        "Analyzing meeting transcript with LLM: model=%s final_chars=%s live_chars=%s title_chars=%s participants=%s prompt_chars=%s",
        OLLAMA_MODEL,
        len(cleaned),
        len(cleaned_supporting),
        len(context_title),
        participant_count,
        len(prompt),
    )
    data = ollama.generate_json(
        prompt=prompt,
        system=SYSTEM,
        response_format=NOTE_JSON_SCHEMA,
    )
    normalized = normalize_note_payload(data)
    logger.info(
        "Normalized AI note payload: title_chars=%s summary_chars=%s context_chars=%s main_topics=%s participant_contributions=%s decisions=%s decision_details=%s action_items=%s risks=%s open_questions=%s open_items=%s tags=%s",
        len(compact_text(normalized.get("title"))),
        len(compact_text(normalized.get("summary"))),
        len(compact_text(normalized.get("context_and_objective"))),
        len(normalized.get("main_topics") or []),
        len(normalized.get("participant_contributions") or []),
        len(normalized.get("decisions") or []),
        len(normalized.get("decision_details") or []),
        len(normalized.get("action_items") or []),
        len(normalized.get("risks") or []),
        len(normalized.get("open_questions") or []),
        len(normalized.get("open_items") or []),
        len(normalized.get("tags") or []),
    )
    if not has_meaningful_note_content(normalized):
        logger.warning(
            "AI note payload had no structured content; using transcript fallback summary: transcript_chars=%s raw_payload_preview=%r",
            len(cleaned),
            json.dumps(data, ensure_ascii=False)[:1000],
        )
        normalized["summary"] = build_fallback_summary(cleaned)
    if not normalized["title"]:
        normalized["title"] = context_title if context_title != "No extra context provided." else "Toplanti notu"
    if not has_meaningful_note_content(normalized):
        logger.error(
            "AI note payload still empty after fallback check: title=%r final_chars=%s live_chars=%s",
            context_title,
            len(cleaned),
            len(cleaned_supporting),
        )
        raise ValueError("LLM output did not contain enough structured meeting note data.")
    return normalized


def analyze_transcript_text_via_map_reduce(
    transcript: str,
    supporting_transcript: str | None = None,
    title: str | None = None,
    participants: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cleaned = transcript.strip()
    if not cleaned:
        raise ValueError("Transcript looks empty.")
    cleaned_supporting = str(supporting_transcript or "").strip()

    ollama = OllamaService(OLLAMA_BASE_URL, OLLAMA_MODEL, timeout=OLLAMA_TIMEOUT)
    context_title = str(title or "").strip() or "No extra context provided."
    participant_context = build_participant_context(participants)
    chunks = split_transcript_into_chunks(cleaned)
    if len(chunks) <= 1:
        logger.info(
            "Map-reduce note pipeline fell back to single pass because chunking produced %s chunk.",
            len(chunks),
        )
        return analyze_transcript_text_single_pass(
            transcript=cleaned,
            supporting_transcript=cleaned_supporting,
            title=title,
            participants=participants,
        )

    logger.info(
        "Using map-reduce note pipeline: model=%s transcript_chars=%s chunk_count=%s target_chars=%s overlap_chars=%s supporting_chars=%s",
        OLLAMA_MODEL,
        len(cleaned),
        len(chunks),
        MAP_REDUCE_CHUNK_TARGET_CHARS,
        MAP_REDUCE_CHUNK_OVERLAP_CHARS,
        len(cleaned_supporting),
    )

    chunk_notes: list[dict[str, Any]] = []
    for index, chunk_text in enumerate(chunks, start=1):
        prompt = CHUNK_EXTRACTION_PROMPT_TEMPLATE.format(
            context_title=context_title,
            participant_context=participant_context,
            chunk_label=f"{index}/{len(chunks)}",
            chunk_text=chunk_text,
        )
        logger.info(
            "Processing transcript chunk for meeting note extraction: chunk=%s/%s chunk_chars=%s prompt_chars=%s",
            index,
            len(chunks),
            len(chunk_text),
            len(prompt),
        )
        try:
            raw_chunk_data = ollama.generate_json(
                prompt=prompt,
                system=SYSTEM,
                response_format=NOTE_JSON_SCHEMA,
            )
            normalized_chunk = normalize_note_payload(raw_chunk_data)
            if not has_meaningful_note_content(normalized_chunk):
                normalized_chunk = build_empty_note_payload(
                    summary=build_fallback_summary(chunk_text, limit=900),
                )
            chunk_notes.append(normalized_chunk)
        except Exception:
            logger.exception(
                "Chunk extraction failed in map-reduce note pipeline: chunk=%s/%s chunk_chars=%s",
                index,
                len(chunks),
                len(chunk_text),
            )
            chunk_notes.append(
                build_empty_note_payload(
                    summary=build_fallback_summary(chunk_text, limit=900),
                )
            )

    merged_material = merge_chunk_note_payloads(chunk_notes, title=context_title)
    structured_materials = {
        "chunk_count": len(chunk_notes),
        "chunk_summaries": dedupe_text_items([item.get("summary") for item in chunk_notes]),
        "merged_note": merged_material,
    }
    if cleaned_supporting:
        structured_materials["supporting_live_transcript_summary"] = build_fallback_summary(
            cleaned_supporting,
            limit=900,
        )

    final_prompt = FINAL_NOTE_FROM_CHUNKS_PROMPT_TEMPLATE.format(
        context_title=context_title,
        participant_context=participant_context,
        structured_materials=json.dumps(structured_materials, ensure_ascii=False, indent=2),
    )
    logger.info(
        "Running final reduction step for map-reduce note pipeline: chunk_count=%s prompt_chars=%s merged_summary_chars=%s merged_topics=%s merged_actions=%s",
        len(chunk_notes),
        len(final_prompt),
        len(compact_text(merged_material.get("summary"))),
        len(merged_material.get("main_topics") or []),
        len(merged_material.get("action_items") or []),
    )

    try:
        reduced = ollama.generate_json(
            prompt=final_prompt,
            system=SYSTEM,
            response_format=NOTE_JSON_SCHEMA,
        )
        normalized = normalize_note_payload(reduced)
    except Exception:
        logger.exception("Final reduction step failed in map-reduce note pipeline.")
        normalized = merged_material

    if not has_meaningful_note_content(normalized):
        logger.warning("Map-reduce note pipeline returned empty content; using merged chunk material as fallback.")
        normalized = merged_material

    if not compact_text(normalized.get("summary")):
        normalized["summary"] = compact_text(merged_material.get("summary")) or build_fallback_summary(cleaned)
    if not compact_text(normalized.get("context_and_objective")):
        normalized["context_and_objective"] = compact_text(merged_material.get("context_and_objective"))
    if not compact_text(normalized.get("title")):
        normalized["title"] = context_title if context_title != "No extra context provided." else "Toplanti notu"

    return normalized


def analyze_transcript_text(
    transcript: str,
    supporting_transcript: str | None = None,
    title: str | None = None,
    participants: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cleaned = transcript.strip()
    if not cleaned:
        raise ValueError("Transcript looks empty.")

    if should_use_map_reduce(cleaned):
        return analyze_transcript_text_via_map_reduce(
            transcript=cleaned,
            supporting_transcript=supporting_transcript,
            title=title,
            participants=participants,
        )

    return analyze_transcript_text_single_pass(
        transcript=cleaned,
        supporting_transcript=supporting_transcript,
        title=title,
        participants=participants,
    )


def analyze_transcript_file(
    input_path: str | Path,
    outdir: str | Path = MEETINGAI_NOTES_DIR,
    title: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    transcript_path = Path(input_path)
    transcript = transcript_path.read_text(encoding="utf-8").strip()
    if not transcript:
        raise ValueError("Transcript looks empty.")

    outdir_path = Path(outdir)
    outdir_path.mkdir(parents=True, exist_ok=True)

    data = analyze_transcript_text(transcript=transcript, title=title)
    created_at = datetime.now()
    stamp = created_at.strftime("%Y%m%d_%H%M%S")
    data["_meta"] = {
        "source_transcript": transcript_path.name,
        "source_transcript_path": str(transcript_path.resolve()),
        "created_at": created_at.isoformat(timespec="seconds"),
    }
    out_json = outdir_path / f"meeting_note_{stamp}.json"
    out_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_json, data


def analyze_meeting(
    store: MeetingStore,
    meeting_id: int,
    title: str | None = None,
    owner_username: str | None = None,
) -> tuple[int, dict[str, Any]]:
    meeting = store.get_meeting(meeting_id, owner_username)
    if meeting is None:
        raise ValueError(f"Meeting not found: {meeting_id}")

    final_transcript = str(meeting.get("final_transcript_text") or "").strip()
    live_transcript = str(meeting.get("raw_text") or "").strip()
    primary_transcript = final_transcript or live_transcript
    if not primary_transcript:
        raise ValueError("Transcript looks empty.")
    supporting_transcript = None
    if final_transcript and live_transcript:
        compact_final = compact_text(final_transcript)
        compact_live = compact_text(live_transcript)
        if compact_live and compact_live != compact_final:
            supporting_transcript = live_transcript

    logger.info(
        "Preparing meeting note generation: meeting=%s owner=%s final_chars=%s live_chars=%s using_final=%s using_supporting_live=%s",
        meeting_id,
        owner_username,
        len(final_transcript),
        len(live_transcript),
        bool(final_transcript),
        bool(supporting_transcript),
    )

    context_title = str(title or meeting.get("title") or "").strip() or None
    participants = store.list_meeting_participants(meeting_id, owner_username)
    data = analyze_transcript_text(
        transcript=primary_transcript,
        supporting_transcript=supporting_transcript,
        title=context_title,
        participants=participants,
    )
    note_id = store.create_note(meeting_id, data, owner_username=owner_username)
    logger.info(
        "Meeting note persisted: meeting=%s note_id=%s title=%r summary_chars=%s",
        meeting_id,
        note_id,
        compact_text(data.get("title")),
        len(compact_text(data.get("summary"))),
    )
    return note_id, data
