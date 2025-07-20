# Applying Live Updates to CocoIndex Flow Example
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

This example demonstrates how to use CocoIndex's live update feature to keep an index synchronized with a local directory.

## How it Works

The `main.py` script defines a CocoIndex flow that:

1.  **Sources** data from a local directory named `data`. It uses a `refresh_interval` of 5 seconds to check for changes.
2.  **Collects** the `filename` and `content` of each file.
3.  **Exports** the collected data to a Postgres database table.

The script then starts a `FlowLiveUpdater`, which runs in the background and continuously monitors the `data` directory for changes.

## Running the Example

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2. **Install the dependencies:**

    ```bash
    pip install -e .
    ```

3.  **Run the example:**

    You can run the live update example in two ways:

    **Option 1: Using the Python script**

    This method uses CocoIndex [Library API](https://cocoindex.io/docs/core/flow_methods#library-api-2) to perform live updates.

    ```bash
    python main.py
    ```

    **Option 2: Using the CocoIndex CLI**

    This method is useful for managing your indexes from the command line, through CocoIndex [CLI](https://cocoindex.io/docs/core/flow_methods#cli-2).

    ```bash
    cocoindex update main.py -L --setup
    ```

4.  **Test the live updates:**

    While the script is running, you can try adding, modifying, or deleting files in the `data` directory. You will see the changes reflected in the logs as CocoIndex updates the index.

## Cleaning Up

To remove the database table created by this example, you can run:

```bash
cocoindex drop main.py
```
