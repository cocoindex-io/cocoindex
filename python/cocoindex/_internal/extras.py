"""Install hints for the package's optional dependencies (its extras)."""


def install_hint(extra: str) -> str:
    """The sentence telling users how to install the dependencies of `extra`.

    Modules that need an optional dependency end the ImportError they raise
    when it's missing with this sentence.
    """
    return f"Please install cocoindex[{extra}]."
