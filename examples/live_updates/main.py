import datetime

import cocoindex
from dotenv import load_dotenv


# Define the flow
@cocoindex.flow_def(name="LiveUpdates")
def live_update_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    # Source: local files in the 'data' directory
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="data"),
        refresh_interval=datetime.timedelta(seconds=5),
    )

    # Collector
    collector = data_scope.add_collector()
    with data_scope["documents"].row() as doc:
        collector.collect(
            filename=doc["filename"],
            content=doc["content"],
        )

    # Target: Postgres database
    collector.export(
        "documents_index",
        cocoindex.targets.Postgres(),
        primary_key_fields=["filename"],
    )


def main() -> None:
    # Setup the flow
    live_update_flow.setup(report_to_stdout=True)

    # Start the live updater
    print("Starting live updater...")
    with cocoindex.FlowLiveUpdater(
        live_update_flow, cocoindex.FlowLiveUpdaterOptions(print_stats=True)
    ) as updater:
        print("Live updater started. Watching for changes in the 'data' directory.")
        print("Try adding, modifying, or deleting files in the 'data' directory.")
        print("Press Ctrl+C to stop.")
        try:
            updater.wait()
        except KeyboardInterrupt:  # handle graceful shutdown
            print("Stopping live updater...")


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    main()
