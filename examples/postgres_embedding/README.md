# PostgreSQL Source Embedding Example 🗄️

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example demonstrates the **PostgreSQL table source** feature in CocoIndex. It reads data from existing PostgreSQL tables, generates embeddings, and stores them in a separate CocoIndex database with pgvector for semantic search.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## What This Example Does

### 📊 Data Flow
```
Source PostgreSQL Table (messages)
    ↓ [Postgres Source]
Text Processing & Embedding Generation
    ↓ [SentenceTransformer]
CocoIndex Database (message_embeddings) with pgvector
    ↓ [Semantic Search]
Query Results
```

### 🔧 Key Features
- **PostgreSQL Source**: Read from existing database tables
- **Separate Databases**: Source data and embeddings stored in different databases
- **Automatic Schema**: CocoIndex creates target tables automatically
- **pgvector Integration**: Store embeddings for semantic search

## Prerequisites

Before running the example, you need to:

1. **PostgreSQL with pgvector**: Follow the [CocoIndex PostgreSQL setup guide](https://cocoindex.io/docs/getting_started/quickstart) to install and configure PostgreSQL with pgvector extension.

2. **Two databases**: You'll need two separate databases (names can be anything you choose):
   - One database for your source table data
   - One database for storing embeddings

3. **Environment file**: Create a `.env` file with your database configuration:
   ```bash
   cp .env.example .env
   $EDITOR .env
   ```

## Installation

Install dependencies:

```bash
pip install -e .
```

## Quick Start

### Environment Variables Explained

The example uses these environment variables to configure the PostgreSQL source:

- **`SOURCE_DATABASE_URL`**: Connection string to your source database containing the table you want to index
- **`COCOINDEX_DATABASE_URL`**: Connection string to the database where CocoIndex will store embeddings
- **`TABLE_NAME`**: Name of the table in your source database to read from
- **`INDEXING_COLUMN`**: The text column to generate embeddings for (this example focuses on one column, but you can index multiple columns)
- **`KEY_COLUMN_FOR_SINGLE_KEY`**: Primary key column name (for tables with single primary key)
- **`KEY_COLUMNS_FOR_MULTIPLE_KEYS`**: Comma-separated primary key columns (for tables with composite primary key)
- **`INCLUDED_COLUMNS`**: Optional - specify which columns to include (defaults to all)
- **`ORDINAL_COLUMN`**: Optional - use for incremental updates

### Option A: Test with Sample Data (Recommended for first-time users)

1. **Setup test database with sample data**:
   ```bash
   python setup_test_database.py
   ```
   This will create both `test_simple` (single primary key) and `test_multiple` (composite primary key) tables with sample data.

2. **Copy the generated environment configuration** to your `.env` file (the script will show you exactly what to copy).

3. **Run the example**:
   ```bash
   python main.py
   ```

4. **Test semantic search** by entering queries in the interactive prompt

### Option B: Use Your Existing Database

1. **Update your `.env` file** with your database URLs and table configuration:
   ```env
   # CocoIndex Database (for storing embeddings)
   COCOINDEX_DATABASE_URL=postgresql://username:password@localhost:5432/cocoindex

   # Source Database (for reading data)
   SOURCE_DATABASE_URL=postgresql://username:password@localhost:5432/your_source_db

   # Table Configuration
   TABLE_NAME=your_table_name
   KEY_COLUMN_FOR_SINGLE_KEY=id  # or KEY_COLUMNS_FOR_MULTIPLE_KEYS=col1,col2
   INDEXING_COLUMN=your_text_column
   ORDINAL_COLUMN=your_timestamp_column  # optional
   ```

2. **Run the example**:
   ```bash
   python main.py
   ```

## How It Works

The example demonstrates a simple flow:

1. **Read from Source**: Uses `cocoindex.sources.PostgresDb` to read from your existing table
2. **Generate Embeddings**: Processes text and creates embeddings using SentenceTransformers
3. **Store Embeddings**: Exports to the CocoIndex database with automatic table creation
4. **Search**: Provides interactive semantic search over the stored embeddings

**Note**: This example indexes one text column for simplicity, but you can modify the flow to index multiple columns or add more complex transformations.

### Key Benefits

- **Separate Databases**: Keep your source data separate from embeddings
- **Automatic Setup**: CocoIndex creates target tables automatically
- **Real-time Updates**: Live updates as source data changes
- **Interactive Search**: Built-in search interface for testing

## Database Configuration

The example uses two separate databases:

1. **Source Database**: Contains your existing data table
2. **CocoIndex Database**: Stores generated embeddings with pgvector support

This separation allows you to:
- Keep your production data unchanged
- Scale embeddings independently
- Use different database configurations for each purpose

## Advanced Usage

### Primary Key Configuration

**Single Primary Key**:
```env
KEY_COLUMN_FOR_SINGLE_KEY=id
```

**Composite Primary Key**:
```env
KEY_COLUMNS_FOR_MULTIPLE_KEYS=product_category,product_name
```



## CocoInsight
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```sh
cocoindex server -ci main.py
```

You can also add a `-L` flag to make the server keep updating the index to reflect source changes at the same time:

```sh
cocoindex server -ci -L main.py
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
