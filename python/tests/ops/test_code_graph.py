from pathlib import Path

import pytest

from cocoindex.ops.code import CodeSource
from cocoindex.ops.code_graph import CodeGraph, extract_code_graph


def test_cpp_graph_preserves_unresolved_references_and_lexical_owners() -> None:
    source = CodeSource(
        """// class Fake { void fake(); };
namespace n {
class Derived : public Base<int>, private other::Base {
    int x, y;
    int method(int a) { obj.call(a); return helper(a); }
};
int helper(int x) { return x; }
int helper(double x) { return 2; }
}
""",
        "cpp",
    )
    graph = extract_code_graph(source, file_path="src/main.cpp")
    assert not graph.has_errors
    names = {e.name for e in graph.entities}
    assert "n::Derived" in names
    assert "n::Derived::x" in names
    assert "n::Derived::y" in names
    assert "n::Derived::method" in names
    assert not any("Fake" in n for n in names)
    overloads = [e for e in graph.entities if e.name == "n::helper"]
    assert len(overloads) == 2
    assert len({e.id for e in overloads}) == 2
    cls = next(e for e in graph.entities if e.name == "n::Derived")
    method = next(e for e in graph.entities if e.name.endswith("::method"))
    assert method.parent_id == cls.id
    bases = [r for r in graph.references if r.kind == "inherits"]
    assert {r.spelling for r in bases} == {"Base<int>", "other::Base"}
    assert all(r.source_id == cls.id for r in bases)
    calls = [r for r in graph.references if r.kind == "call"]
    assert {r.spelling for r in calls} == {"obj.call", "helper"}
    assert all(r.source_id == method.id for r in calls)
    assert all(r.resolution == "unresolved" for r in graph.references)


def test_ids_survive_body_and_position_edits_but_scope_files_and_overloads() -> None:
    before = "struct C { int f(int x) { return missing(x); } };"
    after = "// π\n\nstruct C { int f(int x) { return missing(x) + 123; } };"
    first = extract_code_graph(CodeSource(before, "c++"), file_path="a.cpp")
    second = extract_code_graph(CodeSource(after, "cpp"), file_path="a.cpp")
    assert [e.id for e in first.entities] == [e.id for e in second.entities]
    assert [r.id for r in first.references] == [r.id for r in second.references]
    assert second.entities[-1].start_byte > first.entities[-1].start_byte
    assert second.entities[-1].text == "int f(int x) { return missing(x) + 123; }"
    other = extract_code_graph(CodeSource(before, "cpp"), file_path="b.cpp")
    assert not ({e.id for e in first.entities} & {e.id for e in other.entities})


def test_c_prototype_function_pointer_and_malformed_input() -> None:
    graph = extract_code_graph(
        CodeSource(
            "struct S { int (*cb)(int); int x; }; int f(int); int g(void) { return f(1); }",
            "c",
        ),
        file_path="a.c",
    )
    assert {e.name for e in graph.entities} >= {"S", "S::cb", "S::x", "f", "g"}
    assert next(e for e in graph.entities if e.name == "S::cb").kind == "field"
    assert len(graph.references) == 1
    assert graph.references[0].spelling == "f"
    broken = extract_code_graph(CodeSource("int f( { broken", "cpp"), file_path="a.cpp")
    assert broken.has_errors


@pytest.mark.parametrize(
    "path", ["", "/a.cpp", "../a.cpp", "a/../b.cpp", ".", "a\\b.cpp"]
)
def test_invalid_identity_paths(path: str) -> None:
    with pytest.raises(ValueError, match="file_path"):
        extract_code_graph(CodeSource("", "c"), file_path=path)


def test_unsupported_language() -> None:
    with pytest.raises(ValueError):
        extract_code_graph(CodeSource("def f(): pass", "python"), file_path="a.py")


def test_file_lifecycle_and_parse_memo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import cocoindex as coco
    from cocoindex.connectors import localfs
    from tests import common
    from tests.common.target_states import DictTargetStateStore
    from examples.code_graph_neo4j import pipeline

    store = DictTargetStateStore()
    provider = coco.register_root_target_states_provider("code_graph_lifecycle", store)
    calls: list[str] = []
    original = extract_code_graph

    def counted(source: CodeSource, *, file_path: str) -> CodeGraph:
        calls.append(file_path)
        return original(source, file_path=file_path)

    monkeypatch.setattr(pipeline, "extract_code_graph", counted)
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    a, b = source_dir / "a.cpp", source_dir / "b.cpp"
    a.write_text("int a() { return unknown(); }")
    b.write_text("int b() { return 2; }")

    @coco.fn
    async def store_file(file: localfs.File) -> None:
        path = file.file_path.path.relative_to(source_dir).as_posix()
        graph = await pipeline.parse_file(file, path)
        coco.declare_target_state(provider.target_state(path, graph))

    @coco.fn
    async def main() -> None:
        await coco.mount_each(store_file, localfs.walk_dir(source_dir).items())

    errors: list[str] = []

    def on_error(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        errors.append(str(exc))

    app = coco.App(
        coco.AppConfig(
            name="graph_lifecycle",
            environment=common.create_test_env(__file__, exception_handler=on_error),
        ),
        main,
    )
    app.update_blocking()
    assert sorted(calls) == ["a.cpp", "b.cpp"]
    old_a = store.data["a.cpp"].data
    app.update_blocking()
    assert len(calls) == 2
    a.write_text("int a() { return changed(123); }")
    app.update_blocking()
    assert calls.count("a.cpp") == 2
    assert calls.count("b.cpp") == 1
    updated = store.data["a.cpp"].data
    assert updated.entities[0].id == old_a.entities[0].id
    assert [r.spelling for r in updated.references] == ["changed"]
    a.write_text("int a( { broken")
    app.update_blocking()
    assert len(errors) == 1 and "Parse recovery" in errors[0]
    assert store.data["a.cpp"].data == updated
    a.unlink()
    app.update_blocking()
    assert set(store.data) == {"b.cpp"}
    assert calls.count("b.cpp") == 1
    app.drop_blocking()
    assert not store.data


def test_native_extractor_version_bump_invalidates_unchanged_file(
    tmp_path: Path,
) -> None:
    import cocoindex as coco
    from cocoindex.connectors import localfs
    from tests import common
    from tests.common.module_utils import load_module_as, unload_module_functions
    from tests.common.target_states import DictTargetStateStore
    from examples.code_graph_neo4j import pipeline

    source = Path(pipeline.__file__).read_text()
    version1 = tmp_path / "v1.py"
    version2 = tmp_path / "v2.py"
    version1.write_text(source)
    version2.write_text(source.replace("version=1", "version=2"))
    name = "code_graph_version_test"
    current = [load_module_as(str(version1), name)]
    store = DictTargetStateStore()
    provider = coco.register_root_target_states_provider("code_graph_versions", store)
    file = tmp_path / "input.cpp"
    file.write_text("int a() { return 1; }")

    @coco.fn(memo=True)
    async def save() -> None:
        graph = await current[0].parse_file(
            localfs.File(localfs.FilePath(file)), file.name
        )
        coco.declare_target_state(provider.target_state("graph", graph))

    env = common.create_test_env(__file__, suffix="versions")
    app = coco.App(coco.AppConfig(name="graph_versions", environment=env), save)
    app.update_blocking()
    # The action count alone cannot prove parsing occurred: identical facts
    # correctly produce no external write. Count the native operation instead.
    calls: list[str] = []
    original = extract_code_graph

    def counted(source: CodeSource, *, file_path: str) -> CodeGraph:
        calls.append(file_path)
        return original(source, file_path=file_path)

    setattr(current[0], "extract_code_graph", counted)
    app.update_blocking()
    assert not calls
    unload_module_functions(current[0])
    current[0] = load_module_as(str(version2), name)
    setattr(current[0], "extract_code_graph", counted)
    app.update_blocking()
    assert calls == [file.name]
    app.drop_blocking()
    unload_module_functions(current[0])
