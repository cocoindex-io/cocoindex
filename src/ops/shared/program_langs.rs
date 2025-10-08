use crate::prelude::*;
use unicase::UniCase;

pub struct TreeSitterLanguageInfo {
    pub tree_sitter_lang: tree_sitter::Language,
    pub terminal_node_kind_ids: HashSet<u16>,
}

impl TreeSitterLanguageInfo {
    fn new(
        lang_fn: impl Into<tree_sitter::Language>,
        terminal_node_kinds: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let tree_sitter_lang: tree_sitter::Language = lang_fn.into();
        let terminal_node_kind_ids = terminal_node_kinds
            .into_iter()
            .filter_map(|kind| {
                let id = tree_sitter_lang.id_for_node_kind(kind, true);
                if id != 0 {
                    trace!("Got id for node kind: `{kind}` -> {id}");
                    Some(id)
                } else {
                    error!("Failed in getting id for node kind: `{kind}`");
                    None
                }
            })
            .collect();
        Self {
            tree_sitter_lang,
            terminal_node_kind_ids,
        }
    }
}

pub struct ProgrammingLanguageInfo {
    /// The main name of the language.
    /// It's expected to be consistent with the language names listed at:
    ///   https://github.com/Goldziher/tree-sitter-language-pack?tab=readme-ov-file#available-languages
    pub name: Arc<str>,

    pub treesitter_info: Option<TreeSitterLanguageInfo>,
}

/// Adds a language to the global map of languages.
/// `name` is the main name of the language, used to set the `name` field of the `ProgrammingLanguageInfo`.
/// `aliases` are the other names of the language, which can be language names or file extensions (e.g. `.js`, `.py`).
fn add_treesitter_language(
    output: &mut HashMap<UniCase<&'static str>, Arc<ProgrammingLanguageInfo>>,
    name: &'static str,
    aliases: impl IntoIterator<Item = &'static str>,
    treesitter_info: Option<TreeSitterLanguageInfo>,
) {
    let config = Arc::new(ProgrammingLanguageInfo {
        name: Arc::from(name),
        treesitter_info,
    });
    for name in std::iter::once(name).chain(aliases.into_iter()) {
        if output.insert(name.into(), config.clone()).is_some() {
            panic!("Language `{name}` already exists");
        }
    }
}

static TREE_SITTER_LANGUAGE_BY_LANG: LazyLock<
    HashMap<UniCase<&'static str>, Arc<ProgrammingLanguageInfo>>,
> = LazyLock::new(|| {
    let mut map = HashMap::new();
    add_treesitter_language(
        &mut map,
        "c",
        [".c"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_c::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "cpp",
        [".cpp", ".cc", ".cxx", ".h", ".hpp", "c++"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_cpp::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "csharp",
        [".cs", "cs", "c#"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_c_sharp::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "css",
        [".css", ".scss"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_css::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "fortran",
        [".f", ".f90", ".f95", ".f03", "f", "f90", "f95", "f03"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_fortran::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "go",
        [".go", "golang"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_go::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "html",
        [".html", ".htm"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_html::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "java",
        [".java"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_java::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "javascript",
        [".js", "js"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_javascript::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "json",
        [".json"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_json::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "kotlin",
        [".kt", ".kts"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_kotlin_ng::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "markdown",
        [".md", ".mdx", "md"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_md::LANGUAGE,
            ["inline", "indented_code_block", "fenced_code_block"],
        )),
    );
    add_treesitter_language(
        &mut map,
        "pascal",
        [".pas", "pas", ".dpr", "dpr", "delphi"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_pascal::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "php",
        [".php"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_php::LANGUAGE_PHP,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "python",
        [".py"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_python::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "r",
        [".r"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_r::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "ruby",
        [".rb"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_ruby::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "rust",
        [".rs", "rs"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_rust::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "scala",
        [".scala"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_scala::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "sql",
        [".sql"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_sequel::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "swift",
        [".swift"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_swift::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "toml",
        [".toml"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_toml_ng::LANGUAGE,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "tsx",
        [".tsx"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_typescript::LANGUAGE_TSX,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "typescript",
        [".ts", "ts"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "xml",
        [".xml"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_xml::LANGUAGE_XML,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "dtd",
        [".dtd"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_xml::LANGUAGE_DTD,
            [],
        )),
    );
    add_treesitter_language(
        &mut map,
        "yaml",
        [".yaml", ".yml"],
        Some(TreeSitterLanguageInfo::new(tree_sitter_yaml::LANGUAGE, [])),
    );
    add_treesitter_language(
        &mut map,
        "solidity",
        [".sol"],
        Some(TreeSitterLanguageInfo::new(
            tree_sitter_solidity::LANGUAGE,
            [],
        )),
    );
    map
});

pub fn get_language_info(name: &str) -> Option<&ProgrammingLanguageInfo> {
    TREE_SITTER_LANGUAGE_BY_LANG
        .get(&UniCase::new(name))
        .map(|info| info.as_ref())
}
