# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.3] - 2026-05-05

### Added
- Turbopuffer target connector
- Neo4j connector with meeting_notes_graph_neo4j example
- Per-argument memoization support

## [1.0.2] - 2026-04-29

### Fixed
- Google Drive connector: give each request its own Http to avoid socket races
- Entity resolution: make ResolvedEntities a frozen+slots dataclass for improved performance

## [0.3.39] - 2026-04-29

### Fixed
- Address dependency security vulnerabilities
- Auto generation improvements

### Changed
- Documentation: point v1 to /docs-v1, v0 to /docs (legacy)

## [1.0.1] - 2026-04-28

### Added
- OCI: live bucket watching via LiveStream + OCI Streaming events
- FalkorDB target connector

## [1.0.0] - 2026-04-22

### Added
- Stable 1.0 release
- Updated documentation release environment

### Fixed
- URL in docusaurus config

## [0.3.38] - 2026-04-20

### Fixed
- Make module name consistent
- FTS documentation link in LanceDB

### Changed
- Clarify that collect() accepts plain Python values

---

For older releases and detailed changes, see the [GitHub Releases page](https://github.com/cocoindex-io/cocoindex/releases).
