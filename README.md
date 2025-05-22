# Modern CSV Reader

A high-performance CSV reader application built with Python, pandas, and PyQt6. This application is specifically designed to handle large CSV files (6+ million lines) with a modern dark-themed UI and lag-free scrolling.

## Features

- **High Performance**: Optimized for large files with virtual scrolling and data chunking
- **Multi-threading**: Utilizes your CPU's multi-threading capabilities for faster data loading
- **Modern Dark UI**: Clean, dark-themed interface for comfortable viewing
- **Lag-free Scrolling**: Implements virtual rendering to ensure smooth scrolling even with millions of rows
- **Search Functionality**: Quick search across all columns or specific columns
- **Memory Efficient**: Loads data in chunks to minimize memory usage

## System Requirements

- Python 3.7 or higher
- Dependencies listed in requirements.txt

## Installation

1. Make sure you have Python installed on your system
2. Install the required dependencies:

```
pip install -r requirements.txt
```

## Usage

Run the application with:

```
python csv_reader.py
```

You can also directly open a CSV file by providing its path as an argument:

```
python csv_reader.py path/to/your/file.csv
```

### Opening Files

1. Click the "Open CSV File" button to browse and select a CSV file
2. The application will load the file in chunks, displaying a progress bar
3. Once loaded, you can scroll through the data smoothly

### Searching

1. Enter your search term in the search box
2. Select "All Columns" or a specific column from the dropdown
3. Results will be highlighted and the view will scroll to the first match

## Performance Optimizations

This application implements several optimizations to handle large files efficiently:

- **Data Chunking**: Loads data in manageable chunks rather than all at once
- **Virtual Scrolling**: Only renders the rows currently visible in the viewport
- **Multi-threading**: Uses a separate thread for data loading to keep the UI responsive
- **Debounced Scrolling**: Prevents excessive updates during rapid scrolling
- **Optimized Rendering**: Uses custom delegates for faster cell rendering

## Troubleshooting

If you encounter performance issues:

1. Try adjusting the `CHUNK_SIZE` constant in the code to a value that works better for your system
2. Ensure you have enough available RAM for the application to run efficiently
3. Close other memory-intensive applications while using this CSV reader
