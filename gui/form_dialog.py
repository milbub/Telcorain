from PyQt6 import uic
from PyQt6.QtWidgets import QDialog, QLabel, QLineEdit


class FormDialog(QDialog):
    def __init__(self, parent=None, caption: str = "Enter value", text: str = "Please enter value:"):
        super().__init__(parent)

        # load UI definition from Qt XML file
        uic.loadUi("./gui/FormDialog.ui", self)

        self.question_label = self.findChild(QLabel, "labelQuestion")
        self.answer_box = self.findChild(QLineEdit, "editAnswer")

        self.setWindowTitle(caption)
        self.question_label.setText(text)
