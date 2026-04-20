#!/usr/bin/env python3
"""
Script to generate CLI documentation from CocoIndex Click commands.

This script uses md-click as the foundation but generates enhanced markdown
documentation that's suitable for inclusion in the CocoIndex documentation site.
"""

import sys
from pathlib import Path
import re
import click
from cocoindex.cli import cli

# Add the cocoindex python directory to the path
project_root = Path(__file__).parent.parent
python_path = project_root / "python"
sys.path.insert(0, str(python_path))


def clean_usage_line(usage: str) -> str:
    """Clean up the usage line to remove 'cli' and make it generic, and remove the 'Usage:' prefix."""
    # Replace 'cli' with 'cocoindex' in usage lines and remove 'Usage:' prefix
    cleaned = usage.replace("Usage: cli ", "cocoindex ")
    # Handle case where it might be "Usage: cocoindex" already
    if cleaned.startswith("Usage: cocoindex "):
        cleaned = cleaned.replace("Usage: cocoindex ", "cocoindex ")
    return cleaned


def escape_html_tags(text: str) -> str:
    """Escape HTML-like tags in text to prevent MDX parsing issues, but preserve them in code blocks."""
    import re

    # Handle special cases where URLs with placeholders should be wrapped in code blocks
    text = re.sub(r"http://localhost:<([^>]+)>", r"`http://localhost:<\1>`", text)
    text = re.sub(r"https://([^<\s]+)<([^>]+)>", r"`https://\1<\2>`", text)

    # Handle comma-separated URL examples specifically (e.g., "https://site1.com,http://localhost:3000")
    text = re.sub(r"(?<!`)(\bhttps?://[^\s,`]+,https?://[^\s`]+)(?!`)", r"`\1`", text)

    # Handle standalone URLs that aren't already wrapped in backticks
    text = re.sub(r"(?<!`)(?<!,)(\bhttps?://[^\s,`]+)(?!`)(?!,)", r"`\1`", text)

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
        elif in_description:
            stripped = line.strip()
            if stripped:
                description_lines.append(stripped)
            elif description_lines:  # Preserve blank line only if we have content
                description_lines.append("")

    # Simply join with single newline - let Markdown handle paragraph formatting naturally
    description = "\n".join(description_lines) if description_lines else ""
    return escape_html_tags(description)  # Escape HTML tags for MDX compatibility


def generate_command_docs(cmd: click.Group) -> str:
    """Generate markdown documentation for all commands."""

    markdown_content = []

    ctx = click.core.Context(cmd, info_name=cmd.name)
    subcommands = list(cmd.commands.values())
    # Generate only the command details section (remove redundant headers)
    for sub_cmd in sorted(subcommands, key=lambda x: x.name or ""):
        sub_ctx = click.core.Context(sub_cmd, info_name=sub_cmd.name, parent=ctx)
        command_name = sub_cmd.name
        help_text = sub_cmd.get_help(sub_ctx)
        usage = clean_usage_line(sub_cmd.get_usage(sub_ctx))
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
            markdown_content.append("**Options:**")
            markdown_content.append("")
            markdown_content.append(options_section)

        markdown_content.append("---")
        markdown_content.append("")

    return "\n".join(markdown_content)


# Sentinel markers in cli.mdx delimit the auto-generated subcommand reference.
# The generator splices content between them so TOC headings live in the main
# file (Astro only extracts headings from the page's own MDX AST, not imports).
BEGIN_MARKER = "{/* BEGIN AUTO-GENERATED CLI SUBCOMMANDS"
END_MARKER = "{/* END AUTO-GENERATED CLI SUBCOMMANDS"


def splice_between_markers(existing: str, generated: str) -> str:
    begin_idx = existing.find(BEGIN_MARKER)
    end_idx = existing.find(END_MARKER)
    if begin_idx == -1 or end_idx == -1 or end_idx < begin_idx:
        raise RuntimeError(
            f"Could not find sentinel markers in target file. "
            f"Expected '{BEGIN_MARKER}...' and '{END_MARKER}...'."
        )

    # Keep the BEGIN marker line (and its sibling regenerate-hint comment) by
    # advancing to the end of the BEGIN block. We retain everything up to and
    # including the blank line that follows the BEGIN marker comments.
    begin_line_end = existing.find("\n", begin_idx)
    # Also include the next line if it's a continuation comment (regenerate hint).
    next_line_start = begin_line_end + 1
    next_line_end = existing.find("\n", next_line_start)
    if next_line_end != -1 and existing[
        next_line_start:next_line_end
    ].lstrip().startswith("{/*"):
        begin_block_end = next_line_end + 1
    else:
        begin_block_end = begin_line_end + 1

    prefix = existing[:begin_block_end]
    suffix = existing[end_idx:]
    return f"{prefix}\n## Subcommands Reference\n\n{generated}\n{suffix}"


def main() -> None:
    """Generate CLI documentation and splice into cli.mdx."""
    print("Generating CocoIndex CLI documentation...")

    try:
        generated = generate_command_docs(cli)

        target_file = (
            project_root / "docs" / "src" / "content" / "docs" / "core" / "cli.mdx"
        )
        if not target_file.exists():
            raise RuntimeError(f"Target file not found: {target_file}")

        existing = target_file.read_text(encoding="utf-8")
        updated = splice_between_markers(existing, generated)

        if existing != updated:
            target_file.write_text(updated, encoding="utf-8")
            print(f"CLI documentation updated in: {target_file}")
        else:
            print(f"CLI documentation is up to date in: {target_file}")

    except Exception as e:
        print(f"Error generating documentation: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
