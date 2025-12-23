"""
Format Manager for handling data formatting operations.
"""

import logging
import tempfile
import webbrowser
import os
from typing import Dict, Any, Optional, List


class FormatManager:
    """Manages data formatting operations."""

    def __init__(self, table_manager):
        self.table_manager = table_manager
        self.logger = logging.getLogger(__name__)

    def format_as_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as a table string with column comments.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.table_manager.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Calculate column widths with a maximum limit to prevent overly wide columns
        MAX_COL_WIDTH = 50  # Maximum column width
        MIN_COL_WIDTH = 8   # Minimum column width
        col_widths = {}
        for i, header in enumerate(headers):
            col_widths[i] = min(max(len(str(header)), MIN_COL_WIDTH), MAX_COL_WIDTH)
            for row in data:
                col_value = str(row.get(columns[i], ''))
                # Limit the width of the content when calculating column width
                col_widths[i] = min(max(col_widths[i], len(col_value)), MAX_COL_WIDTH)

        # Create format string
        format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"

        # Create separator
        separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

        # Build table
        lines = [separator]
        lines.append(format_str.format(*headers))
        lines.append(separator)

        for row in data:
            formatted_row = []
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                # Truncate long values for display
                if len(col_value) > MAX_COL_WIDTH:
                    col_value = col_value[:MAX_COL_WIDTH - 3] + "..."
                formatted_row.append(col_value)
            lines.append(format_str.format(*formatted_row))

        lines.append(separator)

        return "\n".join(lines)

    def format_as_ide_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as a table string with adaptive column widths for IDE display.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted table string with adaptive column widths
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.table_manager.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Calculate adaptive column widths based on content
        col_widths = {}
        # First calculate width based on headers
        for i, header in enumerate(headers):
            col_widths[i] = len(str(header))

        # Then adjust based on data content
        for row in data:
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                # Replace newlines with spaces for width calculation
                col_value_single_line = col_value.replace('\n', ' ')
                col_widths[i] = max(col_widths[i], len(col_value_single_line))

        # Apply reasonable maximum width to prevent overly wide columns
        MAX_COL_WIDTH = 100
        for i in col_widths:
            col_widths[i] = min(col_widths[i], MAX_COL_WIDTH)

        # Apply minimum width to ensure proper visibility
        MIN_COL_WIDTH = 8
        for i in col_widths:
            col_widths[i] = max(col_widths[i], MIN_COL_WIDTH)

        # Create format string
        format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"

        # Create separator
        separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

        # Build table
        lines = [separator]
        lines.append(format_str.format(*headers))
        lines.append(separator)

        for row in data:
            formatted_row = []
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                # Replace newlines with spaces for display
                col_value = col_value.replace('\n', ' ')
                formatted_row.append(col_value)
            lines.append(format_str.format(*formatted_row))

        lines.append(separator)

        return "\n".join(lines)

    def format_as_scrollable_html_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as an HTML table string with horizontal scrolling support.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted HTML table string with horizontal scrolling
        """
        if not data:
            return "<!DOCTYPE html><html><head><title>No Data</title></head><body><p>No data to display</p></body></html>"

        # Get column comments
        column_info = self.table_manager.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Start building HTML with horizontal scrolling support
        html_lines = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "    <meta charset=\"UTF-8\">",
            "    <title>Table Data: {}</title>".format(table_name),
            "    <style>",
            "        body { font-family: Arial, sans-serif; margin: 20px; }",
            "        .table-container { overflow-x: auto; white-space: nowrap; }",
            "        table { border-collapse: collapse; width: 100%; }",
            "        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }",
            "        th { background-color: #f2f2f2; font-weight: bold; }",
            "        tr:nth-child(even) { background-color: #f9f9f9; }",
            "        pre { white-space: pre-wrap; word-wrap: break-word; margin: 0; }",
            "    </style>",
            "</head>",
            "<body>",
            "    <h2>Table: {}</h2>".format(table_name),
            "    <div class=\"table-container\">",
            "        <table>",
            "            <thead>",
            "                <tr>"
        ]

        # Add headers
        for header in headers:
            html_lines.append(
                "                    <th>{}</th>".format(header.replace('<', '&lt;').replace('>', '&gt;')))

        html_lines.extend([
            "                </tr>",
            "            </thead>",
            "            <tbody>"
        ])

        # Add data rows
        for row in data:
            html_lines.append("                <tr>")
            for col in columns:
                col_value = str(row.get(col, ''))
                # Escape HTML special characters
                col_value = col_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                # Preserve line breaks
                col_value = col_value.replace('\n', '<br>')
                html_lines.append("                    <td><pre>{}</pre></td>".format(col_value))
            html_lines.append("                </tr>")

        # Close HTML tags
        html_lines.extend([
            "            </tbody>",
            "        </table>",
            "    </div>",
            "</body>",
            "</html>"
        ])

        return "\n".join(html_lines)

    def format_as_html_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as an HTML table string with column comments.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted HTML table string
        """
        if not data:
            return "<!DOCTYPE html><html><head><title>No Data</title></head><body><p>No data to display</p></body></html>"

        # Get column comments
        column_info = self.table_manager.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Start building HTML
        html_lines = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "    <meta charset=\"UTF-8\">",
            "    <title>Table Data: {}</title>".format(table_name),
            "    <style>",
            "        body { font-family: Arial, sans-serif; margin: 20px; }",
            "        table { border-collapse: collapse; width: 100%; }",
            "        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; white-space: pre-wrap; }",
            "        th { background-color: #f2f2f2; font-weight: bold; }",
            "        tr:nth-child(even) { background-color: #f9f9f9; }",
            "        .container { max-width: 100%; overflow-x: auto; }",
            "    </style>",
            "</head>",
            "<body>",
            "    <h2>Table: {}</h2>".format(table_name),
            "    <div class=\"container\">",
            "        <table>",
            "            <thead>",
            "                <tr>"
        ]

        # Add headers
        for header in headers:
            html_lines.append(
                "                    <th>{}</th>".format(header.replace('<', '&lt;').replace('>', '&gt;')))

        html_lines.extend([
            "                </tr>",
            "            </thead>",
            "            <tbody>"
        ])

        # Add data rows
        for row in data:
            html_lines.append("                <tr>")
            for col in columns:
                col_value = str(row.get(col, ''))
                # Escape HTML special characters
                col_value = col_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\n',
                                                                                                              '<br>')
                html_lines.append("                    <td>{}</td>".format(col_value))
            html_lines.append("                </tr>")

        # Close HTML tags
        html_lines.extend([
            "            </tbody>",
            "        </table>",
            "    </div>",
            "</body>",
            "</html>"
        ])

        return "\n".join(html_lines)

    def format_as_vertical_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as a vertical table string, displaying each row as key-value pairs.
        This format is especially useful when there are many columns that don't fit horizontally.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted vertical table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.table_manager.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Calculate maximum key width for alignment
        max_key_width = 0
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())

        for key in all_keys:
            display_key = f"{key}({column_comments[key]})" if key in column_comments else key
            max_key_width = max(max_key_width, len(display_key))

        # Build vertical table
        lines = []
        separator = "-" * (max_key_width + 25)  # Adjust spacing as needed

        for i, row in enumerate(data):
            lines.append(separator)
            lines.append(f"Row {i + 1}:")
            lines.append(separator)

            for key in sorted(row.keys()):  # Sort keys for consistent display
                display_key = f"{key}({column_comments[key]})" if key in column_comments else key
                value = str(row.get(key, ''))

                # Truncate long values for display
                if len(value) > 100:
                    value = value[:97] + "..."

                lines.append(f"{display_key.ljust(max_key_width)} : {value}")
            lines.append("")  # Empty line between rows

        if lines and lines[-1] == "":  # Remove trailing empty line
            lines.pop()

        return "\n".join(lines)

    def format_as_smart_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str, max_columns: int = 10) -> str:
        """
        Smart table formatting that automatically chooses the best format based on column count.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table
            max_columns: Maximum number of columns to show in horizontal format

        Returns:
            str: Formatted table string in the most appropriate format
        """
        if not data:
            return "No data to display"

        # Get column count
        columns = list(data[0].keys())
        column_count = len(columns)

        # Choose format based on column count
        if column_count <= max_columns:
            # Use horizontal format for tables with few columns
            return self.format_as_ide_table(data, connection_name, table_name)
        else:
            # Use vertical format for tables with many columns
            return self.format_as_vertical_table(data, connection_name, table_name)

    def format_as_paged_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str,
                              columns_per_page: int = 8, rows_per_page: int = 20) -> str:
        """
        Format data as a paged table, showing only a subset of columns at a time.
        This is useful for tables with many columns.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table
            columns_per_page: Number of columns to show per page
            rows_per_page: Number of rows to show per page

        Returns:
            str: Formatted paged table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.table_manager.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get all columns
        all_columns = list(data[0].keys())
        total_columns = len(all_columns)
        total_rows = len(data)

        # Calculate number of pages
        column_pages = (total_columns + columns_per_page - 1) // columns_per_page
        row_pages = (total_rows + rows_per_page - 1) // rows_per_page

        # Build paged output
        lines = []
        lines.append(f"Table: {table_name}")
        lines.append(f"Total columns: {total_columns}, Total rows: {total_rows}")
        lines.append(f"Showing {columns_per_page} columns per page, {rows_per_page} rows per page")
        lines.append("=" * 80)

        # Process each column page
        for col_page in range(column_pages):
            start_col = col_page * columns_per_page
            end_col = min(start_col + columns_per_page, total_columns)
            page_columns = all_columns[start_col:end_col]

            lines.append(f"\nColumns {start_col + 1}-{end_col} of {total_columns}:")
            lines.append("-" * 40)

            # Process each row page
            for row_page in range(row_pages):
                start_row = row_page * rows_per_page
                end_row = min(start_row + rows_per_page, total_rows)
                page_data = data[start_row:end_row]

                if row_pages > 1:
                    lines.append(f"\nRows {start_row + 1}-{end_row} of {total_rows}:")

                # Create headers with comments
                headers = []
                for col in page_columns:
                    if col in column_comments:
                        headers.append(f"{col}({column_comments[col]})")
                    else:
                        headers.append(col)

                # Calculate column widths
                col_widths = {}
                for i, header in enumerate(headers):
                    col_widths[i] = len(str(header))
                    for row in page_data:
                        col_value = str(row.get(page_columns[i], ''))
                        col_widths[i] = max(col_widths[i], min(len(col_value), 50))

                # Apply reasonable limits
                MAX_COL_WIDTH = 50
                MIN_COL_WIDTH = 8
                for i in col_widths:
                    col_widths[i] = min(max(col_widths[i], MIN_COL_WIDTH), MAX_COL_WIDTH)

                # Create format string
                format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"
                separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

                # Build table for this page
                lines.append(separator)
                lines.append(format_str.format(*headers))
                lines.append(separator)

                for row in page_data:
                    formatted_row = []
                    for i, col in enumerate(page_columns):
                        col_value = str(row.get(col, ''))
                        if len(col_value) > MAX_COL_WIDTH:
                            col_value = col_value[:MAX_COL_WIDTH - 3] + "..."
                        formatted_row.append(col_value)
                    lines.append(format_str.format(*formatted_row))

                lines.append(separator)

        return "\n".join(lines)

    def format_as_summary_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str,
                                max_columns: int = 6, sample_rows: int = 5) -> str:
        """
        Format data as a summary table, showing only the most important columns and a sample of rows.
        This is useful for getting a quick overview of large datasets.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table
            max_columns: Maximum number of columns to show
            sample_rows: Number of sample rows to show

        Returns:
            str: Formatted summary table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.table_manager.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get all columns and limit to max_columns
        all_columns = list(data[0].keys())
        if len(all_columns) > max_columns:
            columns = all_columns[:max_columns]
            has_more_columns = True
        else:
            columns = all_columns
            has_more_columns = False

        # Limit rows to sample_rows
        if len(data) > sample_rows:
            sample_data = data[:sample_rows]
            has_more_rows = True
        else:
            sample_data = data
            has_more_rows = False

        # Create headers with comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Calculate column widths
        col_widths = {}
        for i, header in enumerate(headers):
            col_widths[i] = len(str(header))
            for row in sample_data:
                col_value = str(row.get(columns[i], ''))
                col_widths[i] = max(col_widths[i], min(len(col_value), 50))

        # Apply reasonable limits
        MAX_COL_WIDTH = 50
        MIN_COL_WIDTH = 8
        for i in col_widths:
            col_widths[i] = min(max(col_widths[i], MIN_COL_WIDTH), MAX_COL_WIDTH)

        # Create format string
        format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"
        separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

        # Build table
        lines = []
        lines.append(f"Table: {table_name}")
        if has_more_columns:
            lines.append(f"[Showing {len(columns)} of {len(all_columns)} columns]")
        if has_more_rows:
            lines.append(f"[Showing {len(sample_data)} of {len(data)} rows]")
        lines.append(separator)
        lines.append(format_str.format(*headers))
        lines.append(separator)

        for row in sample_data:
            formatted_row = []
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                if len(col_value) > MAX_COL_WIDTH:
                    col_value = col_value[:MAX_COL_WIDTH - 3] + "..."
                formatted_row.append(col_value)
            lines.append(format_str.format(*formatted_row))

        lines.append(separator)

        return "\n".join(lines)

    def format_as_html_file(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as HTML table and save to a temporary file.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Path to the HTML file with the table data
        """
        html_content = self.format_as_html_table(data, connection_name, table_name)
        # Create a temporary HTML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
            f.write(html_content)
            temp_filename = f.name

        # Open in browser
        webbrowser.open('file://' + os.path.abspath(temp_filename))

        # Return the path to the file
        return temp_filename