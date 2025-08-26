#!/usr/bin/env python3
"""
Test Database Setup Script for PostgreSQL Source Testing

This script sets up both single and multiple key tables for testing the PostgreSQL source feature.

Usage:
    python setup_test_database.py [options]

Default behavior:
    - Creates both test_simple (single PK) and test_multiple (composite PK) tables
    - Uses postgres://cocoindex:cocoindex@localhost/source_data as default database
    - Always overwrites existing tables (--overwrite is default)

Examples:
    python setup_test_database.py
    python setup_test_database.py --db_url postgres://user:pass@localhost/test_db
"""

import os
import sys
import argparse
from typing import Dict, Any, Optional, List
from psycopg_pool import ConnectionPool
import uuid
from datetime import datetime, timedelta
import random


class DatabaseSchemaSetup:
    """Unified setup for different database schemas"""

    def __init__(self, db_url: str):
        self.db_url = db_url

    def setup_extensions(self):
        """Setup required database extensions"""
        print(f"🔧 Setting up extensions on {self.db_url.split('@')[-1]}...")

        with ConnectionPool(self.db_url) as pool:
            with pool.connection() as conn:
                try:
                    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
                    print("✅ pgvector extension enabled")
                except Exception as e:
                    print(f"⚠️  Warning: Could not enable pgvector: {e}")

                # Enable uuid extension for UUID generation
                try:
                    conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                    print("✅ uuid-ossp extension enabled")
                except Exception as e:
                    print(f"⚠️  Warning: Could not enable uuid-ossp: {e}")

                conn.commit()

    def setup_simple_schema(self, table_name="test_simple"):
        """
        Setup Simple schema with single primary key:
        CREATE TABLE test_simple (
          id uuid NOT NULL PRIMARY KEY,
          message text NOT NULL,
          created_at timestamp DEFAULT CURRENT_TIMESTAMP
        )
        """
        print(f"📝 Setting up Simple schema with table '{table_name}'...")

        with ConnectionPool(self.db_url) as pool:
            with pool.connection() as conn:
                # Always drop existing table for demo
                conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                print(f"🗑️  Dropped existing {table_name} table")

                # Create table
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
                        message text NOT NULL,
                        created_at timestamp DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                print(f"✅ {table_name} table created")

                # Always insert sample data for demo
                sample_messages = [
                    "Hello world! This is a test message.",
                    "PostgreSQL source integration is working great!",
                    "CocoIndex makes database processing so much easier.",
                    "Embeddings and vector search are powerful tools.",
                    "Natural language processing meets database technology.",
                ]

                for message in sample_messages:
                    conn.execute(
                        f"INSERT INTO {table_name} (message) VALUES (%s) ON CONFLICT DO NOTHING",
                        (message,),
                    )

                conn.commit()
                print(f"✅ Inserted {len(sample_messages)} sample messages")

                # Print all inserted rows
                print(f"\n📋 Sample messages inserted:")
                result = conn.execute(
                    f"SELECT id, message, created_at FROM {table_name} ORDER BY created_at"
                )
                for row in result.fetchall():
                    print(f"   ID: {row[0]}, Message: {row[1]}, Created: {row[2]}")

    def setup_multiple_schema(self, table_name="test_multiple"):
        """
        Setup Multiple schema with composite primary key:
        CREATE TABLE test_multiple (
          product_category text NOT NULL,
          product_name text NOT NULL,
          description text,
          price double precision,
          amount integer,
          modified_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (product_category, product_name)
        )
        """
        print(f"📝 Setting up Multiple schema with table '{table_name}'...")

        with ConnectionPool(self.db_url) as pool:
            with pool.connection() as conn:
                # Always drop existing table for demo
                conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                print(f"🗑️  Dropped existing {table_name} table")

                # Create table
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        product_category text NOT NULL,
                        product_name text NOT NULL,
                        description text,
                        price double precision,
                        amount integer,
                        modified_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (product_category, product_name)
                    )
                """)
                print(f"✅ {table_name} table created")

                # Always insert sample data for demo
                sample_products = [
                    (
                        "Electronics",
                        "Wireless Headphones",
                        "High-quality wireless headphones with noise cancellation",
                        199.99,
                        50,
                    ),
                    (
                        "Electronics",
                        "Smartphone",
                        "Latest flagship smartphone with advanced camera",
                        899.99,
                        25,
                    ),
                    (
                        "Electronics",
                        "Laptop",
                        "High-performance laptop for work and gaming",
                        1299.99,
                        15,
                    ),
                    (
                        "Appliances",
                        "Coffee Maker",
                        "Programmable coffee maker with 12-cup capacity",
                        89.99,
                        30,
                    ),
                    (
                        "Sports",
                        "Running Shoes",
                        "Lightweight running shoes for daily training",
                        129.99,
                        60,
                    ),
                ]

                for category, name, desc, price, amount in sample_products:
                    # Add some time variance for testing ordinal columns
                    modified_time = datetime.now() - timedelta(
                        days=random.randint(0, 30)
                    )
                    conn.execute(
                        f"""
                        INSERT INTO {table_name} (product_category, product_name, description, price, amount, modified_time) 
                        VALUES (%s, %s, %s, %s, %s, %s) 
                        ON CONFLICT (product_category, product_name) DO NOTHING
                    """,
                        (category, name, desc, price, amount, modified_time),
                    )

                conn.commit()
                print(f"✅ Inserted {len(sample_products)} sample products")

                # Print all inserted rows
                print(f"\n📋 Sample products inserted:")
                result = conn.execute(
                    f"SELECT product_category, product_name, description, price, amount, modified_time FROM {table_name} ORDER BY product_category, product_name"
                )
                for row in result.fetchall():
                    print(
                        f"   category: {row[0]}, name: {row[1]}, price: ${row[3]}, amount: {row[4]}"
                    )
                    print(f"   description: {row[2]}")
                    print(f"   modified: {row[5]}")
                    print()

    def verify_setup(self, table_name: str):
        """Verify that the setup was successful"""
        print(f"🔍 Verifying setup for table '{table_name}'...")

        with ConnectionPool(self.db_url) as pool:
            with pool.connection() as conn:
                # Check table exists
                result = conn.execute(
                    """
                    SELECT table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """,
                    (table_name,),
                ).fetchall()

                if not result:
                    print(f"❌ Table '{table_name}' not found!")
                    return False

                print(f"✅ Table '{table_name}' structure:")
                for table, column, data_type, nullable in result:
                    null_info = "NULL" if nullable == "YES" else "NOT NULL"
                    print(f"   - {column}: {data_type} ({null_info})")

                # Check primary key
                pk_result = conn.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                        AND tc.table_name = %s
                    ORDER BY kcu.ordinal_position
                """,
                    (table_name,),
                ).fetchall()

                if pk_result:
                    pk_columns = [row[0] for row in pk_result]
                    print(f"✅ Primary key: ({', '.join(pk_columns)})")

                # Check row count
                count_result = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()
                print(f"✅ Row count: {count_result[0]}")

                return True

    def print_connection_info(self):
        """Print connection information for reference"""
        print("\n📋 Database Connection Information:")
        print(f"   Database: {self.db_url.split('@')[-1]}")

    def test_connection(self):
        """Test database connection and provide helpful error messages"""
        try:
            # Use direct psycopg connection instead of pool to avoid retry loops
            import psycopg

            with psycopg.connect(self.db_url, connect_timeout=5) as conn:
                # Test basic connection
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    print("✅ Database connection successful")
                    return True
        except Exception as e:
            error_msg = str(e).lower()

            if "does not exist" in error_msg:
                db_name = self.db_url.split("/")[-1].split("?")[
                    0
                ]  # Remove query params
                print("❌ Database does not exist!")
                print(f"\n🔧 To fix this, create the database '{db_name}' first:")
                print("   1. Connect to PostgreSQL as superuser:")
                print("      psql postgres")
                print("   2. Create the database:")
                print(f"      CREATE DATABASE {db_name};")
                print("   3. Enable pgvector extension:")
                print(f"      \\c {db_name}")
                print("      CREATE EXTENSION IF NOT EXISTS vector;")
                print("   4. Grant permissions to your user:")
                print(
                    f"      GRANT ALL PRIVILEGES ON DATABASE {db_name} TO your_username;"
                )
                print("   5. Try running this script again")

            elif "authentication failed" in error_msg:
                print("❌ Authentication failed!")
                print("\n🔧 Check your .env file:")
                print(
                    "   - Make sure SOURCE_DATABASE_URL includes username and password"
                )
                print(
                    "   - Format: postgresql://username:password@localhost:5432/database"
                )
                print("   - Verify the username and password are correct")

            elif "connection refused" in error_msg:
                print("❌ Connection refused!")
                print("\n🔧 PostgreSQL service is not running:")
                print("   - macOS: brew services start postgresql")
                print("   - Ubuntu: sudo systemctl start postgresql")
                print("   - Check: pg_isready -h localhost -p 5432")

            else:
                print(f"❌ Connection error: {e}")
            print("\n🔧 Check your .env file and database configuration")

            return False

    def create_database(self, db_name=None):
        """Create the database if it doesn't exist"""
        try:
            # Use provided db_name or extract from URL
            if not db_name:
                db_parts = self.db_url.split("/")
                db_name = db_parts[-1].split("?")[0]  # Remove query params

            # Connect to postgres database (default database that always exists)
            postgres_url = self.db_url.rsplit("/", 1)[0] + "/postgres"

            print(f"🔧 Creating database '{db_name}'...")

            import psycopg

            with psycopg.connect(
                postgres_url, connect_timeout=5, autocommit=True
            ) as conn:
                with conn.cursor() as cur:
                    # Create database
                    cur.execute(f"CREATE DATABASE {db_name}")
                    print(f"✅ Database '{db_name}' created successfully")

            # Now enable pgvector extension on the new database
            print(f"🔧 Enabling pgvector extension on '{db_name}'...")
            new_db_url = self.db_url.rsplit("/", 1)[0] + "/" + db_name
            with psycopg.connect(new_db_url, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                    cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                    conn.commit()
                print("✅ Extensions enabled successfully")

            return True

        except Exception as e:
            print(f"❌ Failed to create database: {e}")
            print(f"\n🔧 You may need to:")
            print(f"   1. Connect as superuser: psql postgres")
            print(f"   2. Create manually: CREATE DATABASE {db_name};")
            print(f"   3. Enable extensions: CREATE EXTENSION vector;")
            return False

    def drop_database(self, db_name=None):
        """Drop and recreate the database"""
        try:
            # Use provided db_name or extract from URL
            if not db_name:
                db_parts = self.db_url.split("/")
                db_name = db_parts[-1].split("?")[0]  # Remove query params

            # Connect to postgres database (default database that always exists)
            postgres_url = self.db_url.rsplit("/", 1)[0] + "/postgres"

            print(f"🗑️  Dropping database '{db_name}'...")

            import psycopg

            with psycopg.connect(
                postgres_url, connect_timeout=5, autocommit=True
            ) as conn:
                with conn.cursor() as cur:
                    # Terminate connections to the database
                    cur.execute(f"""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = '{db_name}' AND pid <> pg_backend_pid()
                    """)

                    # Drop database
                    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    print(f"✅ Database '{db_name}' dropped successfully")

            # Now recreate the database
            return self.create_database(db_name)

        except Exception as e:
            print(f"❌ Failed to drop/recreate database: {e}")
            return False


def get_default_config() -> Dict[str, Any]:
    """Get default configuration values"""
    return {
        "source_db_url": "postgres://cocoindex:cocoindex@localhost/source_data",
        "overwrite": True,  # Default to overwrite
    }


def main():
    """Main setup function"""
    parser = argparse.ArgumentParser(
        description="Setup PostgreSQL test database with both single and multiple key tables"
    )
    parser.add_argument(
        "--db_url",
        help="Database URL (default: postgres://cocoindex:cocoindex@localhost/source_data)",
    )
    parser.add_argument(
        "--db_name", help="Database name to create (if using --create_db)"
    )
    parser.add_argument(
        "--create_db", action="store_true", help="Create database if it doesn't exist"
    )
    parser.add_argument(
        "--no_overwrite",
        action="store_true",
        help="Don't overwrite existing tables (default: overwrite)",
    )

    args = parser.parse_args()

    # Get default configuration
    config = get_default_config()

    # Override with command line arguments
    source_db_url = args.db_url or config["source_db_url"]
    overwrite = (
        not args.no_overwrite
    )  # Default to True unless --no_overwrite is specified

    print("🚀 PostgreSQL Test Database Setup for CocoIndex Source Testing")
    print("=" * 70)
    print("📋 This script will create BOTH single and multiple key tables for testing")
    print("📋 Tables: test_simple (single PK) and test_multiple (composite PK)")
    print("=" * 70)

    # Initialize setup for source database
    setup = DatabaseSchemaSetup(source_db_url)
    setup.print_connection_info()

    # Handle database creation if requested
    if args.create_db:
        print("\n🔧 Creating database...")
        if not setup.create_database(args.db_name):
            print("❌ Failed to create database. Exiting.")
            sys.exit(1)
        print("✅ Database created successfully")

    # Test database connection first
    print("\n🔍 Testing database connection...")
    if not setup.test_connection():
        if args.create_db:
            print("\n🔧 Attempting to create database...")
            if setup.create_database(args.db_name):
                print("✅ Database created successfully, testing connection again...")
                if not setup.test_connection():
                    print("❌ Still cannot connect after database creation. Exiting.")
                    sys.exit(1)
            else:
                print("❌ Failed to create database. Exiting.")
                sys.exit(1)
        else:
            print("\n❌ Cannot proceed without a valid database connection.")
            print("   Use --create_db to automatically create the database, or")
            print("   create it manually following the instructions above.")
            sys.exit(1)

    try:
        # Setup extensions
        setup.setup_extensions()

        # Setup both schemas
        print(f"\n📋 Setting up both test tables...")

        # Setup simple schema (single primary key)
        setup.setup_simple_schema("test_simple")

        # Setup multiple schema (composite primary key)
        setup.setup_multiple_schema("test_multiple")

        # Verify both setups
        print(f"\n🔍 Verifying both table setups...")
        success_simple = setup.verify_setup("test_simple")
        success_multiple = setup.verify_setup("test_multiple")

        if success_simple and success_multiple:
            print(f"\n🎉 Test database setup completed successfully!")
            print(f"\n📝 Next steps:")
            print(f"   1. Add addtional environment variables to your .env file")
            print(f"   2. Run: python main.py")

            # Show environment configurations for easy copying
            print(f"\n📋 Environment Configuration for .env file:\n")
            print(f"# Database URLs")
            print(f"SOURCE_DATABASE_URL={source_db_url}")
            print(f"")
            print(f"# ========================================")
            print(f"# Configuration for test_simple table")
            print(f"# ========================================")
            print(f"TABLE_NAME=test_simple")
            print(f"KEY_COLUMN_FOR_SINGLE_KEY=id")
            print(f"INDEXING_COLUMN=message")
            print(f"ORDINAL_COLUMN=created_at")
            print(f"")
            print(f"# ========================================")
            print(f"# Configuration for test_multiple table")
            print(f"# ========================================")
            print(f"TABLE_NAME=test_multiple")
            print(f"KEY_COLUMNS_FOR_MULTIPLE_KEYS=product_category,product_name")
            print(f"INDEXING_COLUMN=description")
            print(f"ORDINAL_COLUMN=modified_time")
            print(f"")
            print(
                f"💡 Pro tip: Use --no_overwrite if you want to keep existing tables!"
            )
        else:
            print("❌ Setup verification failed!")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
