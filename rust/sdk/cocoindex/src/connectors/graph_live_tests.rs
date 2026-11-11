//! Graph target scenarios (Neo4j + FalkorDB) against a live server: self-loops,
//! many relationships per node, batches around the 1000-action transaction
//! boundary, property values of every type, keys with special characters,
//! relationships whose endpoints go away or move, a failed batch followed by a
//! good run, and handles as `use_mount_each!` arguments.
//!
//! Each backend's tests skip when its URI is unset. Run with:
//!   NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=cocoindex \
//!   FALKORDB_URI=falkor://localhost:6379 \
//!     cargo test -p cocoindex --features neo4j,falkordb --lib graph_live_tests
//!
//! They drive the public API only; they live in the crate so the probes can
//! use the `neo4rs` and `redis` clients without making them dev-dependencies
//! of every SDK test build.
//!
//! The same scenarios run on both backends: the test bodies are a macro
//! instantiated once per backend module, which supplies the graph handle and a
//! `count` probe (a query returning one integer).
use std::time::{SystemTime, UNIX_EPOCH};

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

macro_rules! battle_tests {
    () => {
        use cocoindex::{App, Ctx, Environment, Result};
        use serde_json::json;

        use super::nonce;

        use g::{ColumnDef, TableSchema, VectorMetric};

        async fn app(probe: &Probe, name: &str) -> (App, tempfile::TempDir) {
            let dir = tempfile::tempdir().unwrap();
            let app = Environment::builder()
                .db_path(dir.path().join("db"))
                .provide_key(&GRAPH, probe.graph.clone())
                .build()
                .await
                .unwrap()
                .app(name)
                .await
                .unwrap();
            (app, dir)
        }

        fn id_schema() -> TableSchema {
            TableSchema::schemaless("id").unwrap()
        }

        async fn nodes(probe: &Probe, label: &str) -> i64 {
            probe
                .count(&format!("MATCH (n:`{label}`) RETURN count(n)"))
                .await
        }

        async fn edges(probe: &Probe, rel: &str) -> i64 {
            probe
                .count(&format!("MATCH ()-[r:`{rel}`]->() RETURN count(r)"))
                .await
        }

        /// A hub with a self-loop and `leaves` outgoing relationships: more
        /// than two transactions' worth of actions in one table.
        async fn run_hub(app: &App, label: String, rel: String, leaves: usize, loop_weight: i64) {
            app.run(move |ctx| {
                let (label, rel) = (label.clone(), rel.clone());
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    let links =
                        g::mount_relation_target(&ctx, &GRAPH, rel, &nodes, &nodes).await?;
                    nodes.declare_record(&ctx, "hub", &json!({ "id": "hub" }))?;
                    if loop_weight > 0 {
                        links.declare_relation_record(
                            &ctx,
                            "hub",
                            "hub",
                            &json!({ "weight": loop_weight }),
                        )?;
                    }
                    for i in 0..leaves {
                        let id = format!("leaf{i}");
                        nodes.declare_record(&ctx, id.as_str(), &json!({ "id": id }))?;
                        links.declare_relation(&ctx, "hub", id.as_str())?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn self_loop_and_many_relationships_per_node() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("Hub{n}"), format!("LINK{n}"));
            let (app, _dir) = app(&probe, "graph_battle_hub").await;

            run_hub(&app, label.clone(), rel.clone(), 2500, 1).await;
            assert_eq!(nodes(&probe, &label).await, 2501);
            assert_eq!(edges(&probe, &rel).await, 2501);
            let loops = format!(
                "MATCH (h:`{label}` {{id: 'hub'}})-[r:`{rel}`]->(h) WHERE r.weight = 2 RETURN count(r)"
            );
            assert_eq!(probe.count(&loops).await, 0);

            // Half the leaves go, with their relationships; the self-loop is
            // updated in place.
            run_hub(&app, label.clone(), rel.clone(), 1250, 2).await;
            assert_eq!(nodes(&probe, &label).await, 1251);
            assert_eq!(edges(&probe, &rel).await, 1251);
            assert_eq!(probe.count(&loops).await, 1);

            // The self-loop is no longer declared.
            run_hub(&app, label.clone(), rel.clone(), 1250, 0).await;
            assert_eq!(edges(&probe, &rel).await, 1250);
            let any_loop =
                format!("MATCH (h:`{label}`)-[r:`{rel}`]->(h) RETURN count(r)");
            assert_eq!(probe.count(&any_loop).await, 0);

            app.drop_state().await.unwrap();
            assert_eq!(nodes(&probe, &label).await, 0);
            probe.cleanup().await;
        }

        /// Soak: a hub with 20 000 relationships, written, halved and
        /// dropped. Slow; run with the live-test command in the module docs
        /// plus `-- --ignored large_hub_soak`.
        #[tokio::test]
        #[ignore = "slow soak; see the doc comment"]
        async fn large_hub_soak() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("Soak{n}"), format!("SOAK{n}"));
            let (app, _dir) = app(&probe, "graph_battle_soak").await;
            let started = std::time::Instant::now();
            run_hub(&app, label.clone(), rel.clone(), 20_000, 1).await;
            assert_eq!(nodes(&probe, &label).await, 20_001);
            assert_eq!(edges(&probe, &rel).await, 20_001);
            let written = started.elapsed();
            run_hub(&app, label.clone(), rel.clone(), 10_000, 1).await;
            assert_eq!(edges(&probe, &rel).await, 10_001);
            app.drop_state().await.unwrap();
            assert_eq!(nodes(&probe, &label).await, 0);
            eprintln!("soak: wrote 40 001 actions in {written:?}, all in {:?}", started.elapsed());
            probe.cleanup().await;
        }

        async fn run_versioned(app: &App, label: String, count: usize, version: i64) {
            app.run(move |ctx| {
                let label = label.clone();
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    for i in 0..count {
                        nodes.declare_record(
                            &ctx,
                            i as i64,
                            &json!({ "id": i, "version": version }),
                        )?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }

        /// Upserts, then mixed upserts and deletes, at sizes around the
        /// 1000-action transaction boundary.
        #[tokio::test]
        async fn batches_around_the_transaction_boundary() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let label = format!("Batch{}", nonce());
            let (app, _dir) = app(&probe, "graph_battle_batch").await;
            for (version, count) in [(1, 999), (2, 1000), (3, 1001), (4, 2001), (5, 500), (6, 0)] {
                run_versioned(&app, label.clone(), count, version).await;
                assert_eq!(nodes(&probe, &label).await, count as i64, "count {count}");
                let stale = format!(
                    "MATCH (n:`{label}`) WHERE n.version <> {version} RETURN count(n)"
                );
                assert_eq!(probe.count(&stale).await, 0, "every node at version {version}");
            }
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        /// Property values of every type the graph stores, read back with
        /// equality predicates.
        #[tokio::test]
        async fn property_values_of_every_type_round_trip() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let label = format!("Props{}", nonce());
            let (app, _dir) = app(&probe, "graph_battle_props").await;
            let text = "quote ' double \" back \\ tick ` nl \n tab \t ü 日本 🙂";
            let row = json!({
                "id": "p",
                "yes": true,
                "no": false,
                "big": i64::MAX,
                "small": i64::MIN,
                "zero": 0,
                "pi": 3.25,
                "tiny": 1e-300,
                "huge": 1e300,
                "text": text,
                "empty": "",
                "tags": ["a", "b'c"],
                "ints": [1, 2, 3],
                "floats": [0.5, -1.5],
                "none": null,
            });
            app.run({
                let (label, row) = (label.clone(), row.clone());
                move |ctx| {
                    let (label, row) = (label.clone(), row.clone());
                    async move {
                        let nodes =
                            g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                        nodes.declare_record(&ctx, "p", &row)
                    }
                }
            })
            .await
            .unwrap();
            let escaped = text
                .replace('\\', "\\\\")
                .replace('\'', "\\'")
                .replace('\n', "\\n")
                .replace('\t', "\\t");
            let check = format!(
                "MATCH (n:`{label}` {{id: 'p'}}) WHERE n.yes = true AND n.no = false \
                 AND n.big = {} AND n.small = {} AND n.zero = 0 AND n.pi = 3.25 \
                 AND n.tiny = 1e-300 AND n.huge = 1e300 AND n.text = '{escaped}' \
                 AND n.empty = '' AND n.tags = ['a', 'b\\'c'] AND n.ints = [1, 2, 3] \
                 AND n.floats = [0.5, -1.5] AND n.none IS NULL RETURN count(n)",
                i64::MAX,
                i64::MIN,
            );
            assert_eq!(probe.count(&check).await, 1, "{check}");
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        async fn declare_one(app: &App, label: String, row: serde_json::Value) -> Result<()> {
            app.run(move |ctx| {
                let (label, row) = (label.clone(), row.clone());
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    nodes.declare_record(&ctx, "x", &row)
                }
            })
            .await
            .map(|_| ())
        }

        /// Values the graph cannot store exactly fail the run, and are not
        /// written as something else.
        #[tokio::test]
        async fn unrepresentable_values_fail_instead_of_changing() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let label = format!("Bad{}", nonce());
            let (app, _dir) = app(&probe, "graph_battle_bad_values").await;
            // Refused by the SDK before anything is written, on both servers.
            for (row, what) in [
                (json!({ "id": "x", "n": u64::MAX }), "18446744073709551615"),
                (json!({ "id": "x", "m": { "a": 1 } }), "is a map"),
                (json!({ "id": "x", "l": [1, null] }), "holding a null"),
                (json!({ "id": "x", "l": [{ "a": 1 }] }), "holding a map"),
                (json!({ "id": "x", "l": [[1], [null]] }), "holding a null"),
            ] {
                let err = declare_one(&app, label.clone(), row.clone())
                    .await
                    .expect_err(&row.to_string());
                assert!(err.to_string().contains(what), "{row}: {err}");
            }
            // Nested and mixed-type lists: Neo4j refuses them, FalkorDB
            // stores them.
            for row in [
                json!({ "id": "x", "l": [[1.0], [2.0]] }),
                json!({ "id": "x", "l": [1, "a"] }),
            ] {
                let result = declare_one(&app, label.clone(), row.clone()).await;
                assert_eq!(result.is_err(), NEO4J, "{row}: {result:?}");
            }
            if !NEO4J {
                declare_one(&app, label.clone(), json!({ "id": "x" })).await.unwrap();
                app.drop_state().await.unwrap();
            }
            let nan = app
                .run({
                    let label = label.clone();
                    move |ctx| {
                        let label = label.clone();
                        async move {
                            let nodes =
                                g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                            nodes.declare_record(
                                &ctx,
                                "x",
                                &serde_json::Map::from_iter([
                                    ("id".to_string(), json!("x")),
                                    ("v".to_string(), json!(1.0)),
                                ]),
                            )?;
                            #[derive(serde::Serialize)]
                            struct Row {
                                id: &'static str,
                                v: Vec<f32>,
                            }
                            nodes.declare_record(
                                &ctx,
                                "y",
                                &Row {
                                    id: "y",
                                    v: vec![1.0, f32::NAN],
                                },
                            )
                        }
                    }
                })
                .await
                .expect_err("NaN");
            assert!(nan.to_string().contains("NaN") || nan.to_string().contains("finite"), "{nan}");
            // None of the refused rows was written.
            assert_eq!(nodes(&probe, &label).await, 0);
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        const ODD_IDS: &[&str] = &[
            "a'b",
            "back\\slash",
            "new\nline",
            "`tick`",
            "ü日本🙂",
            "s:x",
            "42",
            "}) DETACH DELETE n //",
        ];

        async fn run_odd_ids(app: &App, label: String, rel: String, ids: &'static [&'static str]) {
            app.run(move |ctx| {
                let (label, rel) = (label.clone(), rel.clone());
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    let links =
                        g::mount_relation_target(&ctx, &GRAPH, rel, &nodes, &nodes).await?;
                    nodes.declare_record(&ctx, 42_i64, &json!({ "id": 42, "kind": "int" }))?;
                    for id in ids {
                        nodes.declare_record(&ctx, *id, &json!({ "id": id, "kind": "str" }))?;
                        links.declare_relation(&ctx, *id, 42_i64)?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }

        /// String keys with quotes, escapes, Unicode and Cypher syntax are
        /// stored verbatim, are distinct from an integer key with the same
        /// digits, and reconcile without duplicates.
        #[tokio::test]
        async fn string_keys_with_special_characters() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("Odd{n}"), format!("ODD{n}"));
            let (app, _dir) = app(&probe, "graph_battle_odd_ids").await;
            for _ in 0..2 {
                run_odd_ids(&app, label.clone(), rel.clone(), ODD_IDS).await;
                assert_eq!(nodes(&probe, &label).await, ODD_IDS.len() as i64 + 1);
                assert_eq!(edges(&probe, &rel).await, ODD_IDS.len() as i64);
                let int_node = format!(
                    "MATCH (n:`{label}`) WHERE n.id = 42 AND n.kind = 'int' RETURN count(n)"
                );
                assert_eq!(probe.count(&int_node).await, 1);
                let str_node = format!(
                    "MATCH (n:`{label}`) WHERE n.id = '42' AND n.kind = 'str' RETURN count(n)"
                );
                assert_eq!(probe.count(&str_node).await, 1);
                let quoted = format!(
                    "MATCH (n:`{label}`) WHERE n.id = 'a\\'b' AND n.kind = 'str' RETURN count(n)"
                );
                assert_eq!(probe.count(&quoted).await, 1);
            }
            run_odd_ids(&app, label.clone(), rel.clone(), &ODD_IDS[..2]).await;
            assert_eq!(nodes(&probe, &label).await, 3);
            assert_eq!(edges(&probe, &rel).await, 2);
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        async fn run_pair(app: &App, label: String, rel: String, people: &'static [&'static str]) {
            app.run(move |ctx| {
                let (label, rel) = (label.clone(), rel.clone());
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    let links =
                        g::mount_relation_target(&ctx, &GRAPH, rel, &nodes, &nodes).await?;
                    for p in people {
                        nodes.declare_record(&ctx, *p, &json!({ "id": p, "named": true }))?;
                    }
                    links.declare_relation(&ctx, "a", "b")?;
                    Ok(())
                }
            })
            .await
            .unwrap();
        }

        /// A relationship still declared after its endpoint node no longer
        /// is: deleting the node (`DETACH DELETE`) takes the relationship
        /// along, and as the relationship's own declaration did not change, a
        /// later run does not write it again. Python's connectors behave the
        /// same. This pins the behavior; the gap is that the relationship's
        /// tracking then says it exists while the graph has no such edge.
        #[tokio::test]
        async fn undeclared_endpoint_takes_its_relationships_along() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("End{n}"), format!("END{n}"));
            let (app, _dir) = app(&probe, "graph_battle_endpoint").await;
            run_pair(&app, label.clone(), rel.clone(), &["a", "b"]).await;
            assert_eq!((nodes(&probe, &label).await, edges(&probe, &rel).await), (2, 1));
            for _ in 0..2 {
                run_pair(&app, label.clone(), rel.clone(), &["a"]).await;
                assert_eq!((nodes(&probe, &label).await, edges(&probe, &rel).await), (1, 0));
            }
            // Declaring the endpoint again does not bring the edge back
            // either: only a changed relationship is written.
            run_pair(&app, label.clone(), rel.clone(), &["a", "b"]).await;
            assert_eq!((nodes(&probe, &label).await, edges(&probe, &rel).await), (2, 0));
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        /// An empty-string key: Neo4j stores it; FalkorDB's index skips empty
        /// strings (a `MERGE` would duplicate the node), so the batch fails
        /// before writing anything.
        #[tokio::test]
        async fn empty_string_key() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("Empty{n}"), format!("EMPTY{n}"));
            let (app, _dir) = app(&probe, "graph_battle_empty_key").await;
            let run = || {
                let (label, rel) = (label.clone(), rel.clone());
                app.run(move |ctx| {
                    let (label, rel) = (label.clone(), rel.clone());
                    async move {
                        let nodes =
                            g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                        let links =
                            g::mount_relation_target(&ctx, &GRAPH, rel, &nodes, &nodes).await?;
                        nodes.declare_record(&ctx, "", &json!({ "id": "" }))?;
                        nodes.declare_record(&ctx, "a", &json!({ "id": "a" }))?;
                        links.declare_relation(&ctx, "a", "")
                    }
                })
            };
            for _ in 0..2 {
                let result = run().await;
                if NEO4J {
                    result.unwrap();
                    assert_eq!((nodes(&probe, &label).await, edges(&probe, &rel).await), (2, 1));
                } else {
                    let err = result.expect_err("FalkorDB refuses an empty-string key");
                    assert!(err.to_string().contains("empty string"), "{err}");
                    assert_eq!(nodes(&probe, &label).await, 0);
                }
            }
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        /// A record id that differs from the row's primary key is refused.
        #[tokio::test]
        async fn record_id_must_match_the_row_primary_key() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let label = format!("Mismatch{}", nonce());
            let (app, _dir) = app(&probe, "graph_battle_mismatch").await;
            let err = app
                .run({
                    let label = label.clone();
                    move |ctx| {
                        let label = label.clone();
                        async move {
                            let nodes =
                                g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                            nodes.declare_record(&ctx, "a", &json!({ "id": "b" }))
                        }
                    }
                })
                .await
                .expect_err("mismatched id");
            assert!(err.to_string().contains("differs"), "{err}");
            // The same digits as an integer and as a string differ too.
            let err = declare_one(&app, label.clone(), json!({ "id": 1 }))
                .await
                .expect_err("x vs 1");
            assert!(err.to_string().contains("differs"), "{err}");
            assert_eq!(nodes(&probe, &label).await, 0);
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        async fn run_keyed_edge(app: &App, label: String, rel: String, to: &'static str, extra: bool) {
            app.run(move |ctx| {
                let (label, rel) = (label.clone(), rel.clone());
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    let links = g::mount_relation_target_with_schema(
                        &ctx,
                        &GRAPH,
                        rel,
                        &nodes,
                        &nodes,
                        TableSchema::new(
                            [("eid", ColumnDef::new("STRING")), ("w", ColumnDef::new("INTEGER"))],
                            "eid",
                        )?,
                    )
                    .await?;
                    for p in ["a", "b", "c"] {
                        nodes.declare_record(&ctx, p, &json!({ "id": p }))?;
                    }
                    links.declare_relation_record(&ctx, "a", to, &json!({ "eid": "e1", "w": 1 }))?;
                    if extra {
                        // A second edge between the same endpoints, under its
                        // own key: a multigraph keeps both.
                        links.declare_relation_record(&ctx, "a", to, &json!({ "eid": "e2", "w": 2 }))?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }

        /// A keyed relationship whose endpoint changes moves (one edge, not
        /// two); two keys between the same endpoints are two edges.
        #[tokio::test]
        async fn keyed_relationship_moves_and_parallel_edges() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("Mv{n}"), format!("MV{n}"));
            let (app, _dir) = app(&probe, "graph_battle_move").await;
            run_keyed_edge(&app, label.clone(), rel.clone(), "b", true).await;
            assert_eq!(edges(&probe, &rel).await, 2);
            run_keyed_edge(&app, label.clone(), rel.clone(), "c", false).await;
            assert_eq!(edges(&probe, &rel).await, 1);
            let moved = format!(
                "MATCH (:`{label}` {{id: 'a'}})-[r:`{rel}` {{eid: 'e1'}}]->(:`{label}` {{id: 'c'}}) RETURN count(r)"
            );
            assert_eq!(probe.count(&moved).await, 1);
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        /// Declaring the same endpoint-derived relationship twice in one
        /// component is a duplicate declaration.
        #[tokio::test]
        async fn duplicate_relationship_in_one_component_fails() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("Dup{n}"), format!("DUP{n}"));
            let (app, _dir) = app(&probe, "graph_battle_dup").await;
            let result = app
                .run(move |ctx| {
                    let (label, rel) = (label.clone(), rel.clone());
                    async move {
                        let nodes =
                            g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                        let links =
                            g::mount_relation_target(&ctx, &GRAPH, rel, &nodes, &nodes).await?;
                        links.declare_relation(&ctx, "a", "b")?;
                        links.declare_relation(&ctx, "a", "b")
                    }
                })
                .await;
            assert!(result.is_err(), "a duplicate declaration fails");
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        async fn run_with_bad_row(
            app: &App,
            label: String,
            count: usize,
            bad_at: Option<usize>,
        ) -> Result<()> {
            app.run(move |ctx| {
                let label = label.clone();
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    for i in 0..count {
                        let (key, row) = if Some(i) == bad_at {
                            bad_row(i)
                        } else {
                            (i.to_string(), json!({ "id": i.to_string(), "ok": true }))
                        };
                        nodes.declare_record(&ctx, key.as_str(), &row)?;
                    }
                    Ok(())
                }
            })
            .await
            .map(|_| ())
        }

        /// A row that passes the SDK's checks but fails when it is written.
        fn bad_row(i: usize) -> (String, serde_json::Value) {
            if NEO4J {
                // A key over the index entry size limit fails the uniqueness
                // constraint's index.
                let key = "k".repeat(40_000);
                (key.clone(), json!({ "id": key, "ok": true }))
            } else {
                // A property name FalkorDB's parameter header cannot carry.
                (i.to_string(), json!({ "id": i.to_string(), "ok": true, "bad key": 1 }))
            }
        }

        /// A batch that fails after its first transaction committed fails the
        /// run; the next run (with the row fixed) writes every record.
        #[tokio::test]
        async fn failed_batch_then_successful_run() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let label = format!("Fail{}", nonce());
            let (app, _dir) = app(&probe, "graph_battle_fail").await;
            let err = run_with_bad_row(&app, label.clone(), 1500, Some(1200)).await;
            assert!(err.is_err(), "the bad row fails the run");
            assert!(nodes(&probe, &label).await <= 1500);
            run_with_bad_row(&app, label.clone(), 1500, None).await.unwrap();
            assert_eq!(nodes(&probe, &label).await, 1500);
            let ok = format!("MATCH (n:`{label}`) WHERE n.ok = true RETURN count(n)");
            assert_eq!(probe.count(&ok).await, 1500);
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        #[cocoindex::function]
        async fn link_to_hub(
            ctx: &Ctx,
            id: i64,
            nodes: g::TableTarget,
            links: g::RelationTarget,
        ) -> Result<()> {
            nodes.declare_record(ctx, id, &json!({ "id": id }))?;
            links.declare_relation(ctx, id, "hub")
        }

        async fn run_spokes(app: &App, label: String, rel: String, spokes: i64) {
            app.run(move |ctx| {
                let (label, rel) = (label.clone(), rel.clone());
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    let links =
                        g::mount_relation_target(&ctx, &GRAPH, rel, &nodes, &nodes).await?;
                    nodes.declare_record(&ctx, "hub", &json!({ "id": "hub" }))?;
                    let items: Vec<(String, i64)> =
                        (0..spokes).map(|i| (i.to_string(), i)).collect();
                    cocoindex::use_mount_each!(items, |id| {
                        link_to_hub(ctx, id, nodes.clone(), links.clone())
                    })
                    .await?;
                    Ok(())
                }
            })
            .await
            .unwrap();
        }

        /// Table and relation handles as `use_mount_each!` arguments: many
        /// components write relationships to one hub at once (concurrent
        /// writers of one node), and removing components removes their
        /// records.
        #[tokio::test]
        async fn handles_as_mount_each_arguments() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let n = nonce();
            let (label, rel) = (format!("Spoke{n}"), format!("SPOKE{n}"));
            let (app, _dir) = app(&probe, "graph_battle_spokes").await;
            for _ in 0..2 {
                run_spokes(&app, label.clone(), rel.clone(), 64).await;
                assert_eq!(nodes(&probe, &label).await, 65);
                assert_eq!(edges(&probe, &rel).await, 64);
            }
            run_spokes(&app, label.clone(), rel.clone(), 16).await;
            assert_eq!(nodes(&probe, &label).await, 17);
            assert_eq!(edges(&probe, &rel).await, 16);
            app.drop_state().await.unwrap();
            probe.cleanup().await;
        }

        async fn run_vector(
            app: &App,
            label: String,
            dimension: u32,
            metric: VectorMetric,
            vector: Vec<f32>,
        ) -> Result<()> {
            app.run(move |ctx| {
                let (label, vector) = (label.clone(), vector.clone());
                async move {
                    let nodes = g::mount_table_target(&ctx, &GRAPH, label, id_schema()).await?;
                    nodes.declare_vector_index(&ctx, "embedding", dimension, metric)?;
                    nodes.declare_record(&ctx, "v", &json!({ "id": "v", "embedding": vector }))
                }
            })
            .await
            .map(|_| ())
        }

        /// A vector index whose dimension or metric changes between runs is
        /// rebuilt; a vector of another length is stored (the graph does not
        /// check it against the index).
        #[tokio::test]
        async fn vector_index_dimension_and_metric_change() {
            let Some(probe) = Probe::connect().await else {
                return;
            };
            let label = format!("Vec{}", nonce());
            let (app, _dir) = app(&probe, "graph_battle_vector").await;
            run_vector(&app, label.clone(), 3, VectorMetric::Cosine, vec![1.0, 0.0, 0.0])
                .await
                .unwrap();
            assert_eq!(probe.vector_index_dimension(&label, "embedding").await, Some(3));
            run_vector(&app, label.clone(), 4, VectorMetric::Cosine, vec![1.0, 0.0, 0.0, 0.0])
                .await
                .unwrap();
            assert_eq!(probe.vector_index_dimension(&label, "embedding").await, Some(4));
            run_vector(&app, label.clone(), 4, VectorMetric::Euclidean, vec![1.0, 0.0])
                .await
                .unwrap();
            assert_eq!(probe.vector_index_dimension(&label, "embedding").await, Some(4));
            run_vector(&app, label.clone(), 4, VectorMetric::Euclidean, vec![])
                .await
                .unwrap();
            app.drop_state().await.unwrap();
            assert_eq!(probe.vector_index_dimension(&label, "embedding").await, None);
            probe.cleanup().await;
        }
    };
}

#[cfg(feature = "neo4j")]
mod neo4j_battle {
    use cocoindex::connectors::neo4j as g;

    cocoindex::context_key!(static GRAPH: g::Graph, key = "graph_battle_neo4j");

    const NEO4J: bool = true;

    pub struct Probe {
        graph: g::Graph,
        client: neo4rs::Graph,
    }

    impl Probe {
        async fn connect() -> Option<Self> {
            let Ok(uri) = std::env::var("NEO4J_URI") else {
                eprintln!("skipping live Neo4j test; NEO4J_URI is not set");
                return None;
            };
            let user = std::env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
            let password =
                std::env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "cocoindex".to_string());
            let database = std::env::var("NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".to_string());
            let graph = g::Graph::connect(&uri, &user, &password, &database)
                .await
                .unwrap();
            let host = uri
                .strip_prefix("bolt://")
                .or_else(|| uri.strip_prefix("neo4j://"))
                .unwrap_or(&uri);
            let config = neo4rs::ConfigBuilder::default()
                .uri(host)
                .user(user)
                .password(password)
                .db(database)
                .build()
                .unwrap();
            let client = neo4rs::Graph::connect(config).await.unwrap();
            Some(Self { graph, client })
        }

        /// `cypher` ends with the `RETURN` of one integer.
        async fn count(&self, cypher: &str) -> i64 {
            let mut rows = self
                .client
                .execute(neo4rs::query(&format!("{cypher} AS c")))
                .await
                .unwrap_or_else(|e| panic!("{cypher}: {e}"));
            let row = rows.next().await.unwrap().expect("one row");
            row.get::<i64>("c").unwrap()
        }

        async fn vector_index_dimension(&self, label: &str, field: &str) -> Option<i64> {
            let mut rows = self
                .client
                .execute(neo4rs::query(&format!(
                    "SHOW VECTOR INDEXES YIELD labelsOrTypes, properties, options \
                     WHERE labelsOrTypes = ['{label}'] AND properties = ['{field}'] \
                     RETURN options.indexConfig['vector.dimensions'] AS d"
                )))
                .await
                .unwrap();
            let row = rows.next().await.unwrap()?;
            Some(row.get::<i64>("d").unwrap())
        }

        /// Each test's labels are unique; `drop_state` removed their data.
        async fn cleanup(&self) {}
    }

    battle_tests!();
}

#[cfg(feature = "falkordb")]
mod falkordb_battle {
    use cocoindex::connectors::falkordb as g;

    cocoindex::context_key!(static GRAPH: g::Graph, key = "graph_battle_falkordb");

    const NEO4J: bool = false;

    pub struct Probe {
        graph: g::Graph,
        conn: redis::aio::MultiplexedConnection,
        name: String,
    }

    fn int(value: &redis::Value) -> Option<i64> {
        match value {
            redis::Value::Int(i) => Some(*i),
            redis::Value::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
            redis::Value::SimpleString(s) => s.parse().ok(),
            _ => None,
        }
    }

    impl Probe {
        async fn connect() -> Option<Self> {
            let Ok(uri) = std::env::var("FALKORDB_URI") else {
                eprintln!("skipping live FalkorDB test; FALKORDB_URI is not set");
                return None;
            };
            // One graph per test: tests run in parallel.
            let name = format!("graph_battle_{}", super::nonce());
            let graph = g::Graph::connect(&uri, &name).await.unwrap();
            let redis_uri = uri
                .strip_prefix("falkor://")
                .map(|rest| format!("redis://{rest}"))
                .unwrap_or(uri);
            let conn = redis::Client::open(redis_uri)
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            Some(Self { graph, conn, name })
        }

        async fn query(&self, cypher: &str) -> Vec<Vec<redis::Value>> {
            let mut conn = self.conn.clone();
            let value: redis::Value = redis::cmd("GRAPH.QUERY")
                .arg(&self.name)
                .arg(cypher)
                .query_async(&mut conn)
                .await
                .unwrap_or_else(|e| panic!("{cypher}: {e}"));
            let redis::Value::Array(top) = value else {
                panic!("unexpected reply {value:?}");
            };
            match top.get(1) {
                Some(redis::Value::Array(rows)) => rows
                    .iter()
                    .map(|row| match row {
                        redis::Value::Array(cells) => cells.clone(),
                        other => panic!("unexpected row {other:?}"),
                    })
                    .collect(),
                _ => Vec::new(),
            }
        }

        async fn count(&self, cypher: &str) -> i64 {
            let rows = self.query(cypher).await;
            int(&rows[0][0]).unwrap_or_else(|| panic!("{cypher}: {rows:?}"))
        }

        async fn vector_index_dimension(&self, label: &str, field: &str) -> Option<i64> {
            let rows = self
                .query(&format!(
                    "CALL db.indexes() YIELD label, properties, types, options \
                     WHERE label = '{label}' AND 'VECTOR' IN types['{field}'] \
                     RETURN options['{field}']['dimension']"
                ))
                .await;
            rows.first().map(|row| int(&row[0]).expect("dimension"))
        }

        async fn cleanup(&self) {
            let mut conn = self.conn.clone();
            let _: redis::RedisResult<redis::Value> = redis::cmd("GRAPH.DELETE")
                .arg(&self.name)
                .query_async(&mut conn)
                .await;
        }
    }

    battle_tests!();
}
