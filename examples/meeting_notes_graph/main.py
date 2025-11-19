"""
This example shows how to extract relationships from Markdown documents and build a knowledge graph.
"""

from dataclasses import dataclass
import datetime
import cocoindex
import os

conn_spec = cocoindex.add_auth_entry(
    "Neo4jConnection",
    cocoindex.targets.Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="cocoindex",
    ),
)


@dataclass
class Person:
    name: str


@dataclass
class Task:
    description: str
    assigned_to: list[Person]


@dataclass
class Meeting:
    time: datetime.date
    note: str
    organizer: Person
    participants: list[Person]
    tasks: list[Task]


@cocoindex.flow_def(name="MeetingNotesGraph")
def meeting_notes_graph_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that extracts triples from files and build knowledge graph.
    """
    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.GoogleDrive(
            service_account_credential_path=credential_path,
            root_folder_ids=root_folder_ids,
            recent_changes_poll_interval=datetime.timedelta(seconds=10),
        ),
        refresh_interval=datetime.timedelta(minutes=1),
    )

    meeting_nodes = data_scope.add_collector()
    attended_rels = data_scope.add_collector()
    decided_tasks_rels = data_scope.add_collector()
    assigned_rels = data_scope.add_collector()

    with data_scope["documents"].row() as document:
        document["meetings"] = document["content"].transform(
            cocoindex.functions.SplitBySeparators(
                separators_regex=[r"\n\n##?\ "], keep_separator="RIGHT"
            )
        )
        with document["meetings"].row() as meeting:
            parsed = meeting["parsed"] = meeting["text"].transform(
                cocoindex.functions.ExtractByLlm(
                    llm_spec=cocoindex.LlmSpec(
                        api_type=cocoindex.LlmApiType.OPENAI, model="gpt-5"
                    ),
                    output_type=Meeting,
                )
            )
            meeting_key = {"note_file": document["filename"], "time": parsed["time"]}
            meeting_nodes.collect(**meeting_key, note=parsed["note"])

            attended_rels.collect(
                id=cocoindex.GeneratedField.UUID,
                **meeting_key,
                person=parsed["organizer"]["name"],
                is_organizer=True,
            )
            with parsed["participants"].row() as participant:
                attended_rels.collect(
                    id=cocoindex.GeneratedField.UUID,
                    **meeting_key,
                    person=participant["name"],
                )

            with parsed["tasks"].row() as task:
                decided_tasks_rels.collect(
                    id=cocoindex.GeneratedField.UUID,
                    **meeting_key,
                    description=task["description"],
                )
                with task["assigned_to"].row() as assigned_to:
                    assigned_rels.collect(
                        id=cocoindex.GeneratedField.UUID,
                        **meeting_key,
                        task=task["description"],
                        person=assigned_to["name"],
                    )

    meeting_nodes.export(
        "meeting_nodes",
        cocoindex.targets.Neo4j(
            connection=conn_spec, mapping=cocoindex.targets.Nodes(label="Meeting")
        ),
        primary_key_fields=["note_file", "time"],
    )
    flow_builder.declare(
        cocoindex.targets.Neo4jDeclaration(
            connection=conn_spec,
            nodes_label="Person",
            primary_key_fields=["name"],
        )
    )
    flow_builder.declare(
        cocoindex.targets.Neo4jDeclaration(
            connection=conn_spec,
            nodes_label="Task",
            primary_key_fields=["description"],
        )
    )
    attended_rels.export(
        "attended_rels",
        cocoindex.targets.Neo4j(
            connection=conn_spec,
            mapping=cocoindex.targets.Relationships(
                rel_type="ATTENDED",
                source=cocoindex.targets.NodeFromFields(
                    label="Person",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="person", target="name"
                        )
                    ],
                ),
                target=cocoindex.targets.NodeFromFields(
                    label="Meeting",
                    fields=[
                        cocoindex.targets.TargetFieldMapping("note_file"),
                        cocoindex.targets.TargetFieldMapping("time"),
                    ],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )
    decided_tasks_rels.export(
        "decided_tasks_rels",
        cocoindex.targets.Neo4j(
            connection=conn_spec,
            mapping=cocoindex.targets.Relationships(
                rel_type="DECIDED",
                source=cocoindex.targets.NodeFromFields(
                    label="Meeting",
                    fields=[
                        cocoindex.targets.TargetFieldMapping("note_file"),
                        cocoindex.targets.TargetFieldMapping("time"),
                    ],
                ),
                target=cocoindex.targets.NodeFromFields(
                    label="Task",
                    fields=[
                        cocoindex.targets.TargetFieldMapping("description"),
                    ],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )
    assigned_rels.export(
        "assigned_rels",
        cocoindex.targets.Neo4j(
            connection=conn_spec,
            mapping=cocoindex.targets.Relationships(
                rel_type="ASSIGNED_TO",
                source=cocoindex.targets.NodeFromFields(
                    label="Person",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="person", target="name"
                        ),
                    ],
                ),
                target=cocoindex.targets.NodeFromFields(
                    label="Task",
                    fields=[
                        cocoindex.targets.TargetFieldMapping(
                            source="task", target="description"
                        ),
                    ],
                ),
            ),
        ),
        primary_key_fields=["id"],
    )
