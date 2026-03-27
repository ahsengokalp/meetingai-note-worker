# meetingai-note-worker

MeetingAI transcriptlerinden yapilandirilmis toplanti notu ureten worker katmani.

## Icerik

- Ollama generate / structured output entegrasyonu
- Toplanti notu olusturma
- Mail gonderim mantigi

## Dahili bagimliliklar

- `meetingai-shared`

## Dis bagimliliklar

- `requests`

## Entrypoint

- CLI: `meetingai-note-worker`
- Module: `python -m meetingai_note_worker --meeting-id 123 --owner bt.stajyer`

## Not

Bu paket `meetingai_shared.repositories.MeetingStore` uzerinden calisir. Ayrı repoya tasindiginda `meetingai-shared` paketiyle birlikte kurulmasi gerekir.
