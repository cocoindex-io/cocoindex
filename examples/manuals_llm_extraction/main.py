import tempfile
import dataclasses

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
        self._converter = PdfConverter(
            create_model_dict(), config=config_parser.generate_config_dict()
        )

    def __call__(self, content: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_file:
            temp_file.write(content)
            temp_file.flush()
            text, _, _ = text_from_rendered(self._converter(temp_file.name))
            return text


@dataclasses.dataclass
class ArgInfo:
    """Information about an argument of a method."""

    name: str
    description: str


@dataclasses.dataclass
class MethodInfo:
    """Information about a method."""

    name: str
    args: list[ArgInfo]
    description: str


@dataclasses.dataclass
class ClassInfo:
    """Information about a class."""

    name: str
    description: str
    methods: list[MethodInfo]


@dataclasses.dataclass
class ModuleInfo:
    """Information about a Python module."""

    title: str
    description: str
    classes: list[ClassInfo]
    methods: list[MethodInfo]


@dataclasses.dataclass
class ModuleSummary:
    """Summary info about a Python module."""

    num_classes: int
    num_methods: int


@cocoindex.op.function()
def summarize_module(module_info: ModuleInfo) -> ModuleSummary:
    """Summarize a Python module."""
    return ModuleSummary(
        num_classes=len(module_info.classes),
        num_methods=len(module_info.methods),
    )


@cocoindex.flow_def(name="ManualExtraction")
def manual_extraction_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
):
    """
    Define an example flow that extracts manual information from a Markdown.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="manuals", binary=True)
    )

    modules_index = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["markdown"] = doc["content"].transform(PdfToMarkdown())
        doc["module_info"] = doc["markdown"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    api_type=cocoindex.LlmApiType.OLLAMA,
                    # See the full list of models: https://ollama.com/library
                    model="llama3.2",
                ),
                # Replace by this spec below, to use OpenAI API model instead of ollama
                #   llm_spec=cocoindex.LlmSpec(
                #       api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"),
                # Replace by this spec below, to use Gemini API model
                #   llm_spec=cocoindex.LlmSpec(
                #       api_type=cocoindex.LlmApiType.GEMINI, model="gemini-2.0-flash"),
                # Replace by this spec below, to use Anthropic API model
                #   llm_spec=cocoindex.LlmSpec(
                #       api_type=cocoindex.LlmApiType.ANTHROPIC, model="claude-3-5-sonnet-latest"),
                output_type=ModuleInfo,
                instruction="Please extract Python module information from the manual.",
            )
        )
        doc["module_summary"] = doc["module_info"].transform(summarize_module)
        modules_index.collect(
            filename=doc["filename"],
            module_info=doc["module_info"],
            module_summary=doc["module_summary"],
        )

    modules_index.export(
        "modules",
        cocoindex.targets.Postgres(table_name="modules_info"),
        primary_key_fields=["filename"],
    )
