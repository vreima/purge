import typer
from tinydb import TinyDB, Query
from tinydb.operations import increment
from pathlib import Path
from typing_extensions import Annotated
from datetime import datetime, timezone, date

def purge_file(file: Path, db: TinyDB):
    size = file.stat().st_size
    timestamp = datetime.now(timezone.utc).astimezone()

    document = {
        "ts": timestamp.isoformat(),
        "date": timestamp.date().isoformat(),
        "file": str(file),
        "ext": file.suffix,
        "size": size,
        "batch": 0,
    }

    try:
        # file.unlink()
        pass
    except PermissionError as err:
        db.insert(document | {"error": err.errno})
    except:
        db.insert(document | {"error": True})
        raise
    else:
        db.insert(document)


def update_batch_numbers(db: TinyDB):
    db.update(increment("batch"), Query().date == date.today().isoformat())
    


def get_latest_batch(db: TinyDB):
    Deletion = Query()
    docs = db.search(
        (Deletion.date == date.today().isoformat()) & (Deletion.batch == 0)
    )

    no_errors = [doc for doc in docs if doc.get("error") is None]
    errors = [doc for doc in docs if doc.get("error") is not None]
    extensions: set[str] = {doc["ext"] for doc in docs}

    if no_errors:
        print(f"Deleted {len(no_errors)} files ({sum(doc["size"] for doc in no_errors) / 1024 / 1024:.2f} MB)")

        for extension in extensions:
            subdocs = [doc for doc in docs if doc["ext"] == extension]
            if subdocs:
                print(f"--- {extension.upper()}: {len(subdocs)} files ({sum(doc["size"] for doc in subdocs) / 1024 / 1024:.2f} MB)")

    if errors:
        print(f"Errors in {len(errors)} files.")


def purge_files(dir: Path, ext: str):
    files = dir.glob(f"*.{ext}")

    yield from files


def main(
    dir: Annotated[
        Path,
        typer.Argument(
            exists=True,
            dir_okay=True,
            file_okay=False,
            readable=True,
            writable=True,
            resolve_path=True,
        ),
    ],
    ext: Annotated[list[str], typer.Option()],
    db: Annotated[
        Path,
        typer.Option(
            dir_okay=False,
            file_okay=True,
            readable=True,
            writable=True,
            resolve_path=True,
        ),
    ] = Path.home()
    / ".purge"
    / "db.json",
):
    # Ensure the DB file exists
    db.parent.mkdir(parents=True, exist_ok=True)
    db.touch(exist_ok=True)

    tinydb = TinyDB(db)
    print("toimii:", dir, db, ext)
    update_batch_numbers(tinydb)

    for extension in ext:
        for file in purge_files(dir, extension):
            purge_file(file, tinydb)

    get_latest_batch(tinydb)


if __name__ == "__main__":
    typer.run(main)
