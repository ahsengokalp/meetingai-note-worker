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

## Calistirma

Bu servis kendi repo kokunden calistirilmalidir ve kendi `.env` dosyasini kullanir.

1. Sanal ortami hazirla:
   `python -m venv .venv`
2. Ortak paketi kur:
   `.\.venv\Scripts\python -m pip install -e ..\meetingai_shared`
3. Worker paketini kur:
   `.\.venv\Scripts\python -m pip install -e .`
4. `.env.example` dosyasini `.env` olarak kopyalayip DB, Ollama ve SMTP degerlerini doldur.
5. Servisi baslat:
   `.\.venv\Scripts\Activate.ps1`
   `python main.py --port 5053`

Varsayilan note cikti klasoru bu repo icindedir:
- `data/notes`

## Not

Bu paket `meetingai_shared.repositories.MeetingStore` uzerinden calisir. Ayrı repoya tasindiginda `meetingai-shared` paketiyle birlikte kurulmasi gerekir.
