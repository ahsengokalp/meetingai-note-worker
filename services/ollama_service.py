import json
import logging
import time
from typing import Any, Dict, Optional

import requests


logger = logging.getLogger(__name__)


class OllamaService:
    def __init__(self, base_url: str, model: str, timeout: int = 300):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout

    def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: float = 0.2,
        stream: bool = False,
        response_format: Any | None = None,
    ) -> str:
        url = f"{self.base_url}/api/generate"
        payload: Dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": stream,
            "options": {"temperature": temperature},
        }
        if system:
            payload["system"] = system
        if response_format is not None:
            payload["format"] = response_format

        logger.info(
            "Sending Ollama generate request: model=%s stream=%s timeout_seconds=%s prompt_chars=%s system_chars=%s has_format=%s",
            self.model,
            stream,
            self.timeout,
            len(prompt),
            len(system or ""),
            response_format is not None,
        )
        started = time.perf_counter()

        if not stream:
            try:
                response = requests.post(url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                response_text = (response.json().get("response") or "").strip()
                logger.info(
                    "Ollama generate response received: model=%s status=%s elapsed_seconds=%.2f response_chars=%s",
                    self.model,
                    response.status_code,
                    time.perf_counter() - started,
                    len(response_text),
                )
                return response_text
            except requests.RequestException:
                logger.exception(
                    "Ollama request failed: model=%s elapsed_seconds=%.2f timeout_seconds=%s",
                    self.model,
                    time.perf_counter() - started,
                    self.timeout,
                )
                raise

        try:
            response = requests.post(url, json=payload, stream=True, timeout=self.timeout)
            response.raise_for_status()
            chunks = []
            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue
                obj = json.loads(line)
                if "response" in obj:
                    chunks.append(obj["response"])
                if obj.get("done"):
                    break
            response_text = "".join(chunks).strip()
            logger.info(
                "Ollama streaming response completed: model=%s status=%s elapsed_seconds=%.2f response_chars=%s",
                self.model,
                response.status_code,
                time.perf_counter() - started,
                len(response_text),
            )
            return response_text
        except requests.RequestException:
            logger.exception(
                "Ollama request failed: model=%s elapsed_seconds=%.2f timeout_seconds=%s",
                self.model,
                time.perf_counter() - started,
                self.timeout,
            )
            raise

    def generate_json(
        self,
        prompt: str,
        system: Optional[str] = None,
        response_format: Any | None = "json",
    ) -> Dict[str, Any]:
        out = self.generate(
            prompt=prompt,
            system=system,
            temperature=0.0,
            stream=False,
            response_format=response_format,
        )
        try:
            parsed = json.loads(out)
            logger.info(
                "Ollama JSON parse succeeded: model=%s response_chars=%s",
                self.model,
                len(out),
            )
            return parsed
        except json.JSONDecodeError:
            logger.warning(
                "Ollama JSON parse failed on first attempt: model=%s response_chars=%s response_preview=%r",
                self.model,
                len(out),
                out[:1000],
            )
            repair_system = (
                "Sadece gecerli JSON dondur. Markdown yok. Aciklama yok. "
                "Asagidaki metni gecerli JSON olacak sekilde duzelt."
            )
            repair_prompt = f"METIN:\n{out}\n\nSADECE JSON:"
            fixed = self.generate(
                prompt=repair_prompt,
                system=repair_system,
                temperature=0.0,
                stream=False,
                response_format=response_format,
            )
            try:
                parsed = json.loads(fixed)
                logger.info(
                    "Ollama JSON repair succeeded: model=%s repaired_chars=%s",
                    self.model,
                    len(fixed),
                )
                return parsed
            except json.JSONDecodeError as exc:
                logger.exception(
                    "Ollama JSON repair failed: model=%s original_chars=%s repaired_chars=%s repaired_preview=%r",
                    self.model,
                    len(out),
                    len(fixed),
                    fixed[:1000],
                )
                raise ValueError(
                    "LLM ciktisi gecerli JSON'a donusturulemedi. "
                    f"Orijinal cikti: {out[:500]}"
                ) from exc
