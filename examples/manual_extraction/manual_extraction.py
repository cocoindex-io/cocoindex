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
    args: cocoindex.typing.List[ArgInfo]
    description: str

@dataclasses.dataclass
class ClassInfo:
    name: str
    description: str
    methods: cocoindex.typing.List[MethodInfo]

@dataclasses.dataclass
class ModuleInfo:
    title: str
    description: str
    classes: cocoindex.typing.Table[ClassInfo]
    methods: cocoindex.typing.Table[MethodInfo]


class CleanUpManual(cocoindex.op.FunctionSpec):
    """Clean up manual information."""



@cocoindex.op.executor_class()
class CleanUpManualExecutor:
    """Executor for CleanUpManual."""

    spec: CleanUpManual

    def __call__(self, module_info: ModuleInfo) -> ModuleInfo | None:
        # TODO: Clean up
        return module_info

@cocoindex.flow_def(name="ManualExtraction")
def manual_extraction_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that extracts manual information from a Markdown.
    """
    data_scope["documents"] = flow_builder.add_source(cocoindex.sources.LocalFile(path="manuals", binary=True))

    manual_infos = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["markdown"] = doc["content"].transform(PdfToMarkdown())
        doc["raw_module_info"] = doc["markdown"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.llm.LlmSpec(
                    api_type=cocoindex.llm.LlmApiType.OLLAMA,
                    model="llama3.2:latest"),
                output_type=cocoindex.typing.encode_enriched_type(ModuleInfo),
                instruction="Please extract Python module information from the manual."))
        doc["module_info"] = doc["raw_module_info"].transform(CleanUpManual())
        manual_infos.collect(filename=doc["filename"], module_info=doc["module_info"])

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
