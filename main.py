import argparse
import logging
import sys
from pathlib import Path


if __package__ in {None, ""}:
    repo_parent = Path(__file__).resolve().parent.parent
    if str(repo_parent) not in sys.path:
        sys.path.insert(0, str(repo_parent))


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0", help="Flask bind host")
    parser.add_argument("--port", type=int, default=5053, help="Worker bind port")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    args = parser.parse_args()
    configure_logging()

    try:
        from meetingai_note_worker.app import create_app
    except ModuleNotFoundError as exc:
        if exc.name == "flask":
            raise SystemExit("Flask is not installed. Run: .\\.venv\\Scripts\\pip install Flask") from exc
        raise

    app = create_app()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
