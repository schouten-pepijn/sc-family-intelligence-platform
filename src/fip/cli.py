import typer

app = typer.Typer(help="Family Intelligence Platform CLI.")


@app.callback()
def main_callback() -> None:
    """Family Intelligence Platform command group."""
    return None


def main() -> None:
    app()


# Register command modules; imports must come after `app` is defined.
from fip.commands import cbs as _cbs  # noqa: E402,F401
from fip.commands import gis as _gis  # noqa: E402,F401
from fip.commands import inspection as _inspection  # noqa: E402,F401
from fip.commands import pdok_bag as _pdok_bag  # noqa: E402,F401
