import cocoindex
from markdown_it import MarkdownIt
from datetime import timedelta

_markdown_it = MarkdownIt("gfm-like")


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
