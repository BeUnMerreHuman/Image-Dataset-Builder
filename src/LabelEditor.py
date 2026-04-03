import sys
import os
import pandas as pd
from PyQt6.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QComboBox, QLineEdit, QPushButton, QMessageBox)
from PyQt6.QtCore import Qt

class CSVLabelEditor(QWidget):
    def __init__(self):
        super().__init__()
        self.csv_file = "metadata.csv"    
        self.initUI()
        self.load_csv_data()

    def initUI(self):
        self.setWindowTitle("Label Editor")
        self.resize(450, 150)
        
        self.setStyleSheet("""
            QWidget {
                background-color: #2b2d30;
                color: #a9b7c6;
                font-family: Arial;
                font-size: 14px;
            }
            QLabel {
                font-weight: bold;
                margin-bottom: 2px;
            }
            QComboBox, QLineEdit {
                background-color: #1e1f22;
                color: #a9b7c6;
                border: 1px solid #43454a;
                padding: 6px;
                border-radius: 4px;
            }
            QComboBox::drop-down {
                border-left: 1px solid #43454a;
            }
            QLineEdit::placeholder {
                color: #6a6a6a;
            }
            QPushButton {
                background-color: #365880;
                color: white;
                border: none;
                padding: 8px;
                border-radius: 4px;
                font-weight: bold;
                margin-top: 10px;
            }
            QPushButton:hover {
                background-color: #436a99;
            }
        """)

        main_layout = QVBoxLayout()
        columns_layout = QHBoxLayout()

        col1_layout = QVBoxLayout()
        lbl_select = QLabel("Select Label:")
        self.combo_box = QComboBox()
        col1_layout.addWidget(lbl_select)
        col1_layout.addWidget(self.combo_box)
        col1_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        col2_layout = QVBoxLayout()
        lbl_edit = QLabel("Edit Value:")
        self.line_edit = QLineEdit()
        self.line_edit.setPlaceholderText("Enter New Value") 
        col2_layout.addWidget(lbl_edit)
        col2_layout.addWidget(self.line_edit)
        col2_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        columns_layout.addLayout(col1_layout)
        columns_layout.addLayout(col2_layout)

        self.btn_apply = QPushButton("Apply and Update CSV")
        self.btn_apply.clicked.connect(self.update_csv)
        
        self.line_edit.returnPressed.connect(self.update_csv)

        main_layout.addLayout(columns_layout)
        main_layout.addWidget(self.btn_apply)
        main_layout.addStretch()

        self.setLayout(main_layout)

    def load_csv_data(self):
        try:
            self.df = pd.read_csv(self.csv_file)
            
            if "label" in self.df.columns:
                unique_labels = self.df["label"].dropna().unique()
                self.combo_box.clear()
                self.combo_box.addItems([str(label) for label in unique_labels])
            else:
                QMessageBox.warning(self, "Error", "The column 'label' was not found in the CSV.")
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load CSV: {str(e)}")

    def update_csv(self):
        old_value = self.combo_box.currentText()
        new_value = self.line_edit.text().strip()

        if not new_value:
            QMessageBox.information(self, "Notice", "Please enter a new value in the text box.")
            return
            
        if not old_value:
            return

        try:
            self.df.loc[self.df["label"] == old_value, "label"] = new_value
            
            self.df.to_csv(self.csv_file, index=False)
            
            QMessageBox.information(self, "Success", f"Successfully updated '{old_value}' to '{new_value}'.")
            
            self.line_edit.clear()
            self.load_csv_data()
            
        except Exception as e:
             QMessageBox.critical(self, "Error", f"Failed to update CSV: {str(e)}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = CSVLabelEditor()
    window.show()
    sys.exit(app.exec())