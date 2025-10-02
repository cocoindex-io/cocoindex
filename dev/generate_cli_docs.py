#!/usr/bin/env python3
"""
Script to generate CLI documentation from CocoIndex Click commands.

This script uses md-click as the foundation but generates enhanced markdown
documentation that's suitable for inclusion in the CocoIndex documentation site.
"""

import sys
import os
from pathlib import Path
import re
from typing import Dict, List, Any

# Add the cocoindex python directory to the path
project_root = Path(__file__).parent.parent
python_path = project_root / "python"
sys.path.insert(0, str(python_path))

try:
    import md_click
    from cocoindex.cli import cli
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Make sure to run this script from the project root and install dependencies")
    sys.exit(1)


def clean_usage_line(usage: str) -> str:
    """Clean up the usage line to remove 'cli' and make it generic."""
    # Replace 'cli' with 'cocoindex' in usage lines
    return usage.replace("Usage: cli ", "Usage: cocoindex ")


def escape_html_tags(text: str) -> str:
    """Escape HTML-like tags in text to prevent MDX parsing issues, but preserve them in code blocks."""
    import re

    # Handle special cases where URLs with placeholders should be wrapped in code blocks
    text = re.sub(r"http://localhost:<([^>]+)>", r"`http://localhost:<\1>`", text)
    text = re.sub(r"https://([^<\s]+)<([^>]+)>", r"`https://\1<\2>`", text)

    # Split text into code blocks and regular text
    # Pattern matches: `code content` (inline code blocks)
    parts = re.split(r"(`[^`]*`)", text)

    result = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            # Even indices are regular text, escape HTML tags
            result.append(part.replace("<", "&lt;").replace(">", "&gt;"))
        else:
            # Odd indices are code blocks, preserve as-is
            result.append(part)

    return "".join(result)


def format_options_section(help_text: str) -> str:
    """Extract and format the options section."""
    lines = help_text.split("\n")
    options_start = None
    commands_start = None

    for i, line in enumerate(lines):
        if line.strip() == "Options:":
            options_start = i
        elif line.strip() == "Commands:":
            commands_start = i
            break

    if options_start is None:
        return ""

    # Extract options section
    end_idx = commands_start if commands_start else len(lines)
    options_lines = lines[options_start + 1 : end_idx]  # Skip "Options:" header

    # Parse options - each option starts with exactly 2 spaces and a dash
    formatted_options = []
    current_option = None
    current_description = []

    for line in options_lines:
        if not line.strip():  # Empty line
            continue

        # Check if this is a new option line (starts with exactly 2 spaces then -)
        if line.startswith("  -") and not line.startswith("   "):
            # Save previous option if exists
            if current_option is not None:
                desc = " ".join(current_description).strip()
                desc = escape_html_tags(desc)  # Escape HTML tags for MDX compatibility
                formatted_options.append(f"| `{current_option}` | {desc} |")

            # Remove the leading 2 spaces
            content = line[2:]

            # Find the position where we have multiple consecutive spaces (start of description)
            match = re.search(r"\s{2,}", content)
            if match:
                # Split at the first occurrence of multiple spaces
                option_part = content[: match.start()]
                desc_part = content[match.end() :]
                current_option = option_part.strip()
                current_description = [desc_part.strip()] if desc_part.strip() else []
            else:
                # No description on this line, just the option
                current_option = content.strip()
                current_description = []
        else:
            # Continuation line (starts with more than 2 spaces)
            if current_option is not None and line.strip():
                current_description.append(line.strip())

    # Add last option
    if current_option is not None:
        desc = " ".join(current_description).strip()
        desc = escape_html_tags(desc)  # Escape HTML tags for MDX compatibility
        formatted_options.append(f"| `{current_option}` | {desc} |")

    if formatted_options:
        header = "| Option | Description |\n|--------|-------------|"
        return f"{header}\n" + "\n".join(formatted_options) + "\n"

    return ""


def format_commands_section(help_text: str) -> str:
    """Extract and format the commands section."""
    lines = help_text.split("\n")
    commands_start = None

    for i, line in enumerate(lines):
        if line.strip() == "Commands:":
            commands_start = i
            break

    if commands_start is None:
        return ""

    # Extract commands section
    commands_lines = lines[commands_start + 1 :]

    # Parse commands - each command starts with 2 spaces then the command name
    formatted_commands = []

    for line in commands_lines:
        if not line.strip():  # Empty line
            continue

        # Check if this is a command line (starts with 2 spaces + command name)
        match = re.match(r"^  (\w+)\s{2,}(.+)$", line)
        if match:
            command = match.group(1)
            description = match.group(2).strip()
            # Truncate long descriptions
            if len(description) > 80:
                description = description[:77] + "..."
            formatted_commands.append(f"| `{command}` | {description} |")

    if formatted_commands:
        header = "| Command | Description |\n|---------|-------------|"
        return f"{header}\n" + "\n".join(formatted_commands) + "\n"

    return ""


def extract_description(help_text: str) -> str:
    """Extract the main description from help text."""
    lines = help_text.split("\n")

    # Find the description between usage and options/commands
    description_lines = []
    in_description = False

    for line in lines:
        if line.startswith("Usage:"):
            in_description = True
            continue
        elif line.strip() in ["Options:", "Commands:"]:
            break
        elif in_description and line.strip():
            description_lines.append(line.strip())

    description = "\n\n".join(description_lines) if description_lines else ""
    return escape_html_tags(description)  # Escape HTML tags for MDX compatibility


def generate_command_docs(docs: List[Dict[str, Any]]) -> str:
    """Generate markdown documentation for all commands."""

    # Separate main CLI from subcommands
    main_cli = None
    subcommands = []

    for doc in docs:
        parent = doc.get("parent", "")
        if not parent:
            main_cli = doc
        else:
            subcommands.append(doc)

    markdown_content = []

    if main_cli:
        # Generate main CLI documentation
        help_text = main_cli["help"]
        usage = clean_usage_line(main_cli["usage"])
        description = extract_description(help_text)

        markdown_content.append("# CLI Commands Reference")
        markdown_content.append("")
        markdown_content.append(
            "This page contains the detailed help information for all CocoIndex CLI commands."
        )
        markdown_content.append("")

        if description:
            markdown_content.append(f"## Overview")
            markdown_content.append("")
            markdown_content.append(description)
            markdown_content.append("")

        # Add usage
        markdown_content.append("## Usage")
        markdown_content.append("")
        markdown_content.append(f"```sh")
        markdown_content.append(usage)
        markdown_content.append("```")
        markdown_content.append("")

        # Add global options
        options_section = format_options_section(help_text)
        if options_section:
            markdown_content.append("## Global Options")
            markdown_content.append("")
            markdown_content.append(options_section)
            markdown_content.append("")

        # Add commands overview
        commands_section = format_commands_section(help_text)
        if commands_section:
            markdown_content.append("## Commands")
            markdown_content.append("")
            markdown_content.append(commands_section)
            markdown_content.append("")

    # Generate subcommand documentation
    markdown_content.append("## Command Details")
    markdown_content.append("")

    for doc in sorted(subcommands, key=lambda x: x["command"].name):
        command_name = doc["command"].name
        help_text = doc["help"]
        usage = clean_usage_line(doc["usage"])
        description = extract_description(help_text)

        markdown_content.append(f"### `{command_name}`")
        markdown_content.append("")

        if description:
            markdown_content.append(description)
            markdown_content.append("")

        # Add usage
        markdown_content.append("**Usage:**")
        markdown_content.append("")
        markdown_content.append(f"```bash")
        markdown_content.append(usage)
        markdown_content.append("```")
        markdown_content.append("")

        # Add options if any
        options_section = format_options_section(help_text)
        if options_section:
            # Remove the "## Options" header since it's a subsection
            markdown_content.append("**Options:**")
            markdown_content.append("")
            markdown_content.append(options_section)
            markdown_content.append("")

        markdown_content.append("---")
        markdown_content.append("")

    return "\n".join(markdown_content)


def main():
    """Generate CLI documentation and save to file."""
    print("Generating CocoIndex CLI documentation...")

    try:
        # Generate documentation using md-click
        docs_generator = md_click.main.recursive_help(cli)
        docs = list(docs_generator)

        print(f"Found {len(docs)} CLI commands to document")

        # Generate markdown content
        markdown_content = generate_command_docs(docs)

        # Determine output path
        docs_dir = project_root / "docs" / "docs" / "core"
        output_file = docs_dir / "cli-reference.md"

        # Ensure directory exists
        docs_dir.mkdir(parents=True, exist_ok=True)

        # Write the generated documentation
        content_changed = True
        if output_file.exists():
            with open(output_file, "r", encoding="utf-8") as f:
                existing_content = f.read()
            content_changed = existing_content != markdown_content

        if content_changed:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(markdown_content)

            print(f"CLI documentation generated successfully at: {output_file}")
            print(
                f"Generated {len(markdown_content.splitlines())} lines of documentation"
            )
        else:
            print(f"CLI documentation is up to date at: {output_file}")

    except Exception as e:
        print(f"Error generating documentation: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
