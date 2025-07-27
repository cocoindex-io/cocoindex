from datetime import timedelta
import os
import dataclasses

import cocoindex
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")


class LocalFileTarget(cocoindex.op.TargetSpec):
    """Represents the custom target spec."""

    # The directory to save the HTML files.
    directory: str


@dataclasses.dataclass
class LocalFileTargetValues:
    """Represents value fields of exported data. Used in `mutate` method below."""

    html: str


@cocoindex.op.target_connector(spec_cls=LocalFileTarget)
class LocalFileTargetConnector:
    @staticmethod
    def get_persistent_key(spec: LocalFileTarget, target_name: str) -> str:
        """Use the directory path as the persistent key for this target."""
        return spec.directory

    @staticmethod
    def describe(key: str) -> str:
        """(Optional) Return a human-readable description of the target."""
        return f"Local directory {key}"

    @staticmethod
    def apply_setup_change(
        key: str, previous: LocalFileTarget | None, current: LocalFileTarget | None
    ) -> None:
        """
        Apply setup changes to the target.

        Best practice: keep all actions idempotent.
        """

        # Create the directory if it didn't exist.
        if previous is None and current is not None:
            os.makedirs(current.directory, exist_ok=True)

        # Delete the directory with its contents if it no longer exists.
        if previous is not None and current is None:
            if os.path.isdir(previous.directory):
                for filename in os.listdir(previous.directory):
                    if filename.endswith(".html"):
                        os.remove(os.path.join(previous.directory, filename))
                os.rmdir(previous.directory)

    @staticmethod
    def prepare(spec: LocalFileTarget) -> LocalFileTarget:
        """
        (Optional) Prepare for execution. To run common operations before applying any mutations.
        The returned value will be passed as the first element of tuples in `mutate` method.

        If not provided, will directly pass the spec to `mutate` method.
        """
        return spec

    @staticmethod
    def mutate(
        *all_mutations: tuple[LocalFileTarget, dict[str, LocalFileTargetValues | None]],
    ) -> None:
        """
        Mutate the target.

        The first element of the tuple is the target spec.
        The second element is a dictionary of mutations:
        - The key is the filename, and the value is the mutation.
        - If the value is `None`, the file will be removed.
          Otherwise, the file will be written with the content.

        Best practice: keep all actions idempotent.
        """
        for spec, mutations in all_mutations:
            for filename, mutation in mutations.items():
                full_path = os.path.join(spec.directory, filename) + ".html"
                if mutation is None:
                    try:
                        os.remove(full_path)
                    except FileNotFoundError:
                        pass
                else:
                    with open(full_path, "w") as f:
                        f.write(mutation.html)


@cocoindex.op.function()
def markdown_to_html(text: str) -> str:
    return _markdown_it.render(text)


@cocoindex.flow_def(name="CustomOutputFiles")
def custom_output_files(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that exports markdown files to HTML files.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data", included_patterns=["*.md"]),
        refresh_interval=timedelta(seconds=5),
    )

    output_html = data_scope.add_collector()
    with data_scope["documents"].row() as doc:
        doc["html"] = doc["content"].transform(markdown_to_html)
        output_html.collect(filename=doc["filename"], html=doc["html"])

    output_html.export(
        "OutputHtml",
        LocalFileTarget(directory="output_html"),
        primary_key_fields=["filename"],
    )
