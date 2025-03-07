import tempfile
import dataclasses

from dotenv import load_dotenv
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from marker.config.parser import ConfigParser

import cocoindex

class PdfToMarkdown(cocoindex.op.FunctionSpec):
    """Convert a PDF to markdown."""

@cocoindex.op.executor_class(gpu=True, cache=True, behavior_version=1)
class PdfToMarkdownExecutor:
    """Executor for PdfToMarkdown."""

    spec: PdfToMarkdown
    _converter: PdfConverter

    def prepare(self):
        config_parser = ConfigParser({})
        self._converter = PdfConverter(create_model_dict(), config=config_parser.generate_config_dict())

    def __call__(self, content: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_file:
            temp_file.write(content)
            temp_file.flush()
            text, _, _ = text_from_rendered(self._converter(temp_file.name))
            return text

@dataclasses.dataclass
class ArgInfo:
    name: str
    description: str

@dataclasses.dataclass
class MethodInfo:
    name: str
    args: list[ArgInfo]
    description: str

@dataclasses.dataclass
class ClassInfo:
    name: str
    description: str
    methods: list[MethodInfo]

@dataclasses.dataclass
class ManualInfo:
    title: str
    description: str
    classes: list[ClassInfo]
    methods: list[MethodInfo]


class ExtractManual(cocoindex.op.FunctionSpec):
    """Extract manual information from a Markdown."""

@cocoindex.op.executor_class()
class ExtractManualExecutor:
    """Executor for ExtractManual."""

    spec: ExtractManual

    def __call__(self, _markdown: str) -> ManualInfo:
        return ManualInfo(
            title="title_placeholder",
            description="description_placeholder",
            classes=[
                ClassInfo(
                    name="class_name_placeholder",
                    description="class_description_placeholder",
                    methods=[
                        MethodInfo(
                            name="method_name_placeholder",
                            args=[ArgInfo(name="arg_name_placeholder", description="arg_description_placeholder")],
                            description="method_description_placeholder"
                        )
                    ]
                )
            ],
            methods=[
                MethodInfo(
                    name="method_name_placeholder",
                    args=[ArgInfo(name="arg_name_placeholder", description="arg_description_placeholder")],
                    description="method_description_placeholder"
                )
            ]
        )

@cocoindex.flow_def(name="ManualExtraction")
def manual_extraction_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that extracts manual information from a Markdown.
    """
    data_scope["documents"] = flow_builder.add_source(cocoindex.sources.LocalFile(path="pdf_files", binary=True))

    manual_infos = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["markdown"] = doc["content"].transform(PdfToMarkdown())
        doc["manual_info"] = doc["markdown"].transform(ExtractManual())
        manual_infos.collect(filename=doc["filename"], manual_info=doc["manual_info"])

    manual_infos.export(
        "manual_infos",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename"],
    )

@cocoindex.main_fn()
def _run():
    pass

if __name__ == "__main__":
    load_dotenv(override=True)
    _run()
