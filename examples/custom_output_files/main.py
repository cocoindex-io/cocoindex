import cocoindex
from markdown_it import MarkdownIt
from datetime import timedelta
import os
import dataclasses

_markdown_it = MarkdownIt("gfm-like")


class LocalFileTarget(cocoindex.op.TargetSpec):
    directory: str


@dataclasses.dataclass
class LocalFileTargetValues:
    content: str


@cocoindex.op.target_connector(spec_cls=LocalFileTarget)
class LocalFileTargetExecutor:
    @staticmethod
    def get_persistent_key(spec: LocalFileTarget, target_name: str) -> str:
        return spec.directory

    @staticmethod
    def describe(key: str) -> str:
        return f"Local directory {key}"

    @staticmethod
    def apply_setup_change(
        key: str, previous: LocalFileTarget | None, current: LocalFileTarget | None
    ) -> None:
        if previous is None and current is not None:
            os.makedirs(current.directory, exist_ok=True)

        if previous is not None and current is None:
            for filename in os.listdir(previous.directory):
                if filename.endswith(".html"):
                    os.remove(os.path.join(previous.directory, filename))
            try:
                os.rmdir(previous.directory)
            except (FileExistsError, FileNotFoundError):
                pass

    @staticmethod
    def prepare(spec: LocalFileTarget) -> LocalFileTarget:
        """
        Prepare for execution. To run common operations before applying any mutations.
        The returned value will be passed as the first element of tuples in `mutate` method.

        This is optional. If not provided, will directly pass the spec to `mutate` method.
        """
        return spec

    @staticmethod
    def mutate(
        *all_mutations: tuple[LocalFileTarget, dict[str, LocalFileTargetValues | None]],
    ) -> None:
        """
        Mutate the target.

        The first element of the tuple is the target spec.
        The second element is a dictionary of mutations.
        The key is the filename, and the value is the mutation.
        If the value is `None`, the file will be removed.
        Otherwise, the file will be written with the content.
        """
        for spec, mutations in all_mutations:
            for filename, mutation in mutations.items():
                full_path = os.path.join(spec.directory, filename) + ".html"
                if mutation is None:
                    os.remove(full_path)
                else:
                    with open(full_path, "w") as f:
                        f.write(mutation.content)


@cocoindex.op.function()
def markdown_to_html(text: str) -> str:
    return _markdown_it.render(text)


@cocoindex.flow_def(name="CustomOutputFiles")
def custom_output_files(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds text into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="files", included_patterns=["*.md"]),
        refresh_interval=timedelta(seconds=5),
    )

    output_html = data_scope.add_collector()
    with data_scope["documents"].row() as doc:
        doc["html"] = doc["content"].transform(markdown_to_html)
        output_html.collect(filename=doc["filename"], html=doc["html"])

    output_html.export(
        "output_html",
        cocoindex.targets.LocalFile(directory="output_html"),
    )
