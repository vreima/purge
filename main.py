import typer
from tinydb import TinyDB, Query
from tinydb.operations import increment
from pathlib import Path
from typing import NotRequired, Sequence, TypedDict
from typing_extensions import Annotated
from datetime import datetime, timezone, date


class Document(TypedDict):
    ts: str
    date: str
    file: str
    ext: str
    size: int
    batch: int
    error: NotRequired[bool | int]


app = typer.Typer()


def purge_file(file: Path, db: TinyDB):
    size = file.stat().st_size
    timestamp = datetime.now(timezone.utc).astimezone()

    document: Document = {
        "ts": timestamp.isoformat(),
        "date": timestamp.date().isoformat(),
        "file": str(file),
        "ext": file.suffix,
        "size": size,
        "batch": 0,
    }

    try:
        file.unlink()
    except PermissionError as err:
        db.insert(document | {"error": err.errno})
    except:
        db.insert(document | {"error": True})
        raise
    else:
        db.insert(document)


def update_batch_numbers(db: TinyDB):
    """
    Increment the batch numbers of earlier deletions of today in the database,
    so that the batch number 0 is always the latest deletion.
    """
    db.update(increment("batch"), Query().date == date.today().isoformat())


def get_latest_batch(db: TinyDB):
    Deletion = Query()
    docs = db.search(
        (Deletion.date == date.today().isoformat()) & (Deletion.batch == 0)
    )

    format_query([Document(**doc) for doc in docs])


def format_query(docs: Sequence[Document], show_errors: bool = True):
    """
    Helper function, prettyprints a list of documents (file deletions).
    """
    no_errors = [doc for doc in docs if doc.get("error") is None]
    errors = [doc for doc in docs if doc.get("error") is not None]
    extensions: set[str] = {doc["ext"] for doc in docs}

    if no_errors:
        print(
            f"Deleted {len(no_errors)} files ({sum(doc["size"] for doc in no_errors) / 1024 / 1024:.2f} MB)"
        )

        for extension in extensions:
            subdocs = [doc for doc in docs if doc["ext"] == extension]
            if subdocs:
                print(
                    f"--- {extension.upper()}: {len(subdocs)} files ({sum(doc["size"] for doc in subdocs) / 1024 / 1024:.2f} MB)"
                )

    if show_errors and errors:
        print(f"Errors in {len(errors)} files.")


def purge_dir(path: Path, db: TinyDB):
    for child in path.iterdir():
        if child.is_dir():
            purge_dir(child, db)
        else:
            purge_file(child, db)

    try:
        path.rmdir()
    except OSError as err:
        print(f"Error trying to purge {path}: {err.strerror} [{err.errno}]")


def purge_files(
    root_path: Path, exts: Sequence[str], directory_pattern: Sequence[str], db: TinyDB
):
    # Files
    for ext in exts:
        files = root_path.glob(f"*.{ext}")

        for file in files:
            purge_file(file, db)

    # Directories
    for pattern in directory_pattern:
        dirs = (dir for dir in root_path.glob(f"{pattern}") if dir.is_dir())

        for dir in dirs:
            purge_dir(dir, db)


def open_database(path: Path) -> TinyDB:
    # Ensure the DB file exists
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)

    # Open the db
    tinydb = TinyDB(path)
    update_batch_numbers(tinydb)

    return tinydb


########################
#                      #
# Commands             #
#                      #
########################


@app.command()
def query(
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
    / "db.json"
):
    tinydb = open_database(db)
    format_query([Document(**doc) for doc in tinydb.all()])


@app.command()
def purge(
    exts: Annotated[list[str], typer.Argument()],
    dir: Annotated[
        Path,
        typer.Option(
            exists=True,
            dir_okay=True,
            file_okay=False,
            readable=True,
            writable=True,
            resolve_path=True,
        ),
    ],
    pattern: Annotated[list[str], typer.Option()] = [],
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
    tinydb = open_database(db)

    # Traverse and purge
    purge_files(dir, exts, pattern, tinydb)

    # Print results
    get_latest_batch(tinydb)


if __name__ == "__main__":
    app()
