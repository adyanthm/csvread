import sys
import os
import pandas as pd
import numpy as np
from PyQt6.QtWidgets import (QApplication, QMainWindow, QTableView, QVBoxLayout, 
                             QHBoxLayout, QWidget, QPushButton, QFileDialog, 
                             QLabel, QProgressBar, QSplitter, QComboBox, QLineEdit,
                             QHeaderView, QScrollBar, QMessageBox, QStatusBar, QItemDelegate)
from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex, pyqtSignal, QThread, QTimer
from PyQt6.QtGui import QColor, QPalette, QFont, QIcon

# Constants
CHUNK_SIZE = 100000  # Number of rows to load at once
VISIBLE_ROWS = 100   # Number of rows to display at once

# Worker thread for loading data
class DataLoaderThread(QThread):
    progress_update = pyqtSignal(int)
    data_loaded = pyqtSignal(object, int)
    error_occurred = pyqtSignal(str)
    
    def __init__(self, file_path, chunk_size):
        super().__init__()
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.is_running = True
        
    def run(self):
        try:
            # Get total number of lines for progress calculation
            total_lines = sum(1 for _ in open(self.file_path, 'r'))
            self.progress_update.emit(0)
            
            # Use pandas to read the file in chunks
            chunks_loaded = 0
            for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size):
                if not self.is_running:
                    break
                    
                # Emit the chunk and progress
                chunks_loaded += len(chunk)
                progress = int((chunks_loaded / total_lines) * 100)
                self.data_loaded.emit(chunk, chunks_loaded)
                self.progress_update.emit(progress)
                
            self.progress_update.emit(100)
        except Exception as e:
            self.error_occurred.emit(str(e))
    
    def stop(self):
        self.is_running = False

# Custom table model for virtual scrolling
class DataFrameModel(QAbstractTableModel):
    def __init__(self):
        super().__init__()
        self.df = pd.DataFrame()
        self.display_df = pd.DataFrame()
        self.total_rows = 0
        self.loaded_rows = 0
        self.offset = 0
        self.column_widths = {}
        
    def update_chunk(self, chunk, loaded_rows):
        # First chunk initialization
        if self.df.empty:
            self.df = chunk
        else:
            # Append new chunk to existing data
            self.df = pd.concat([self.df, chunk])
            
        self.loaded_rows = loaded_rows
        self.update_display_data()
        
    def set_total_rows(self, total_rows):
        self.total_rows = total_rows
        
    def update_display_data(self):
        # Update the display dataframe based on current offset
        end_idx = min(self.offset + VISIBLE_ROWS, len(self.df))
        if self.offset < len(self.df):
            self.display_df = self.df.iloc[self.offset:end_idx].copy()
            self.layoutChanged.emit()
        
    def set_offset(self, offset):
        # Set the current offset for virtual scrolling
        self.offset = max(0, min(offset, self.total_rows - VISIBLE_ROWS))
        self.update_display_data()
        
    def rowCount(self, parent=QModelIndex()):
        if parent.isValid() or self.display_df.empty:
            return 0
        return len(self.display_df)
    
    def columnCount(self, parent=QModelIndex()):
        if parent.isValid() or self.display_df.empty:
            return 0
        return len(self.display_df.columns)
    
    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or self.display_df.empty:
            return None
            
        row, col = index.row(), index.column()
        if row >= len(self.display_df) or col >= len(self.display_df.columns):
            return None
            
        value = self.display_df.iloc[row, col]
        
        if role == Qt.ItemDataRole.DisplayRole:
            # Format the value for display
            if pd.isna(value):
                return ""
            elif isinstance(value, (float, np.float64)):
                return f"{value:.6f}"
            else:
                return str(value)
                
        elif role == Qt.ItemDataRole.BackgroundRole:
            # Alternate row colors for better readability
            if row % 2 == 0:
                return QColor(45, 45, 45)
            else:
                return QColor(55, 55, 55)
                
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            # Align numbers to the right, text to the left
            if isinstance(value, (int, float, np.number)):
                return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            return int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                
        return None
        
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
            
        if orientation == Qt.Orientation.Horizontal and not self.display_df.empty:
            return str(self.display_df.columns[section])
            
        if orientation == Qt.Orientation.Vertical and not self.display_df.empty:
            # Show the actual row number in the full dataset
            return str(self.offset + section + 1)
            
        return None

# Custom table view with optimized scrolling
class OptimizedTableView(QTableView):
    scroll_position_changed = pyqtSignal(int)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.verticalScrollBar().valueChanged.connect(self.handle_scroll)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        
        # Optimize rendering
        self.setVerticalScrollMode(QTableView.ScrollMode.ScrollPerPixel)
        self.setHorizontalScrollMode(QTableView.ScrollMode.ScrollPerPixel)
        
        # Improve performance
        self.setItemDelegate(FastDelegate())
        
        # Set selection behavior
        self.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.setSelectionMode(QTableView.SelectionMode.ExtendedSelection)
        
        # Customize appearance
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.verticalHeader().setDefaultSectionSize(25)  # Compact rows
        
        # Timer for delayed scroll updates to prevent excessive model updates
        self.scroll_timer = QTimer(self)
        self.scroll_timer.setSingleShot(True)
        self.scroll_timer.timeout.connect(self.update_model_from_scroll)
        self.current_scroll_value = 0
        
    def handle_scroll(self, value):
        self.current_scroll_value = value
        # Use timer to debounce scroll events
        self.scroll_timer.start(50)  # 50ms delay
        
    def update_model_from_scroll(self):
        # Calculate the row offset based on scroll position
        model = self.model()
        if model and hasattr(model, 'total_rows') and model.total_rows > 0:
            max_scroll = self.verticalScrollBar().maximum()
            if max_scroll > 0:
                # Calculate the row to display at the top based on scroll position
                visible_ratio = self.current_scroll_value / max_scroll
                row_offset = int(visible_ratio * (model.total_rows - VISIBLE_ROWS))
                self.scroll_position_changed.emit(row_offset)

# Fast delegate for improved rendering performance
class FastDelegate(QItemDelegate):
    def paint(self, painter, option, index):
        # Simplified painting for better performance
        super().paint(painter, option, index)

# Main application window
class CSVReaderApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Modern CSV Reader")
        self.resize(1200, 800)
        
        # Set dark theme
        self.setup_dark_theme()
        
        # Initialize UI components
        self.setup_ui()
        
        # Data handling
        self.model = DataFrameModel()
        self.table_view.setModel(self.model)
        self.current_file = None
        self.loader_thread = None
        
        # Show welcome message
        self.status_bar.showMessage("Welcome! Open a CSV file to begin.")
        
    def setup_dark_theme(self):
        # Set application-wide dark theme
        dark_palette = QPalette()
        dark_palette.setColor(QPalette.ColorRole.Window, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.white)
        dark_palette.setColor(QPalette.ColorRole.Base, QColor(35, 35, 35))
        dark_palette.setColor(QPalette.ColorRole.AlternateBase, QColor(45, 45, 45))
        dark_palette.setColor(QPalette.ColorRole.ToolTipBase, QColor(25, 25, 25))
        dark_palette.setColor(QPalette.ColorRole.ToolTipText, Qt.GlobalColor.white)
        dark_palette.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.white)
        dark_palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.white)
        dark_palette.setColor(QPalette.ColorRole.BrightText, Qt.GlobalColor.red)
        dark_palette.setColor(QPalette.ColorRole.Link, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.ColorRole.Highlight, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.black)
        
        # Apply the palette
        self.setPalette(dark_palette)
        
        # Set stylesheet for additional styling
        self.setStyleSheet("""
            QMainWindow, QWidget {
                background-color: #353535;
                color: white;
            }
            QTableView {
                background-color: #252525;
                alternate-background-color: #2d2d2d;
                gridline-color: #3a3a3a;
                color: white;
                selection-background-color: #2a82da;
                selection-color: white;
            }
            QHeaderView::section {
                background-color: #404040;
                color: white;
                padding: 5px;
                border: 1px solid #505050;
            }
            QPushButton {
                background-color: #404040;
                color: white;
                border: 1px solid #505050;
                padding: 5px 10px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #505050;
            }
            QPushButton:pressed {
                background-color: #2a82da;
            }
            QLineEdit, QComboBox {
                background-color: #252525;
                color: white;
                border: 1px solid #505050;
                padding: 3px;
                border-radius: 2px;
            }
            QProgressBar {
                border: 1px solid #505050;
                border-radius: 3px;
                background-color: #252525;
                text-align: center;
                color: white;
            }
            QProgressBar::chunk {
                background-color: #2a82da;
                width: 10px;
            }
            QScrollBar:vertical {
                border: none;
                background-color: #353535;
                width: 12px;
                margin: 0px;
            }
            QScrollBar::handle:vertical {
                background-color: #505050;
                min-height: 20px;
                border-radius: 3px;
            }
            QScrollBar::handle:vertical:hover {
                background-color: #2a82da;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
            QScrollBar:horizontal {
                border: none;
                background-color: #353535;
                height: 12px;
                margin: 0px;
            }
            QScrollBar::handle:horizontal {
                background-color: #505050;
                min-width: 20px;
                border-radius: 3px;
            }
            QScrollBar::handle:horizontal:hover {
                background-color: #2a82da;
            }
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
                width: 0px;
            }
            QSplitter::handle {
                background-color: #505050;
            }
            QStatusBar {
                background-color: #404040;
                color: white;
            }
        """)
        
    def setup_ui(self):
        # Create central widget and main layout
        central_widget = QWidget()
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(5)
        
        # Top controls layout
        top_controls = QHBoxLayout()
        
        # Open file button
        self.open_button = QPushButton("Open CSV File")
        self.open_button.clicked.connect(self.open_file)
        top_controls.addWidget(self.open_button)
        
        # Search controls
        self.search_label = QLabel("Search:")
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Enter search term...")
        self.search_input.textChanged.connect(self.search_data)
        self.search_column = QComboBox()
        self.search_column.addItem("All Columns")
        
        top_controls.addWidget(self.search_label)
        top_controls.addWidget(self.search_input)
        top_controls.addWidget(self.search_column)
        
        # Add top controls to main layout
        main_layout.addLayout(top_controls)
        
        # Progress bar for loading
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        # Table view for data display
        self.table_view = OptimizedTableView()
        self.table_view.scroll_position_changed.connect(self.update_visible_rows)
        main_layout.addWidget(self.table_view)
        
        # Status bar for information
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # Set central widget
        self.setCentralWidget(central_widget)
        
    def open_file(self):
        # Open file dialog to select CSV file
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Open CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        
        if file_path:
            self.load_file(file_path)
    
    def load_file(self, file_path):
        # Stop any existing loader thread
        if self.loader_thread and self.loader_thread.isRunning():
            self.loader_thread.stop()
            self.loader_thread.wait()
        
        # Reset model and UI
        self.model = DataFrameModel()
        self.table_view.setModel(self.model)
        self.current_file = file_path
        self.setWindowTitle(f"Modern CSV Reader - {os.path.basename(file_path)}")
        
        # Update status
        self.status_bar.showMessage(f"Loading {os.path.basename(file_path)}...")
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        
        # Get total number of lines for progress calculation
        try:
            total_lines = sum(1 for _ in open(file_path, 'r'))
            self.model.set_total_rows(total_lines - 1)  # Subtract header row
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not open file: {str(e)}")
            self.progress_bar.setVisible(False)
            return
        
        # Create and start loader thread
        self.loader_thread = DataLoaderThread(file_path, CHUNK_SIZE)
        self.loader_thread.progress_update.connect(self.update_progress)
        self.loader_thread.data_loaded.connect(self.update_data)
        self.loader_thread.error_occurred.connect(self.handle_error)
        self.loader_thread.start()
        
        # Update column selector for search
        self.search_column.clear()
        self.search_column.addItem("All Columns")
        
    def update_progress(self, value):
        # Update progress bar
        self.progress_bar.setValue(value)
        if value == 100:
            self.progress_bar.setVisible(False)
            self.status_bar.showMessage(f"Loaded {os.path.basename(self.current_file)} successfully")
    
    def update_data(self, chunk, loaded_rows):
        # Update model with new data chunk
        self.model.update_chunk(chunk, loaded_rows)
        
        # Update column selector on first chunk
        if self.search_column.count() <= 1 and not chunk.empty:
            for column in chunk.columns:
                self.search_column.addItem(str(column))
    
    def update_visible_rows(self, offset):
        # Update the visible rows based on scroll position
        if self.model:
            self.model.set_offset(offset)
    
    def search_data(self, text):
        # Implement search functionality
        if not text or not self.model or self.model.df.empty:
            return
            
        # Get search column
        column_idx = self.search_column.currentIndex() - 1  # -1 because first item is "All Columns"
        
        try:
            # Perform search
            if column_idx >= 0 and column_idx < len(self.model.df.columns):
                # Search in specific column
                column_name = self.model.df.columns[column_idx]
                mask = self.model.df[column_name].astype(str).str.contains(text, case=False, na=False)
                result_indices = self.model.df[mask].index
            else:
                # Search in all columns
                mask = False
                for col in self.model.df.columns:
                    mask |= self.model.df[col].astype(str).str.contains(text, case=False, na=False)
                result_indices = self.model.df[mask].index
                
            if len(result_indices) > 0:
                # Scroll to first result
                first_result = result_indices[0]
                self.model.set_offset(max(0, first_result - 5))  # Show a few rows above the result
                self.status_bar.showMessage(f"Found {len(result_indices)} matches")
            else:
                self.status_bar.showMessage("No matches found")
                
        except Exception as e:
            self.status_bar.showMessage(f"Search error: {str(e)}")
    
    def handle_error(self, error_msg):
        # Handle errors from loader thread
        QMessageBox.critical(self, "Error", f"Error loading data: {error_msg}")
        self.progress_bar.setVisible(False)
        self.status_bar.showMessage("Error loading file")

# Run the application
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")  # Use Fusion style for better dark theme support
    
    window = CSVReaderApp()
    window.show()
    
    # If a file path is provided as argument, load it
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        window.load_file(sys.argv[1])
    
    sys.exit(app.exec())