"""This module contains utility classes and functions for the application."""
from PyQt6.QtGui import QIcon
from PyQt6.QtWidgets import QLabel, QCheckBox, QMessageBox, QTableWidget
from PyQt6 import QtCore

from database.models.mwlink import MwLink

class LinksTableFactory:
    """
    Factory class for creating and updating the links view table in the Control Widget.
    Non-active rows in the table are hidden. Table is updated with the current selection and is not editable.
    """
    def __init__(self, table: QTableWidget):
        """
        Initialize the factory with the table.

        :param table: QTableWidget object
        """
        self.selection_table = table

        # style out link table
        self.selection_table.setColumnWidth(0, 40)
        self.selection_table.setColumnWidth(1, 42)
        self.selection_table.setColumnWidth(2, 42)
        self.selection_table.setColumnWidth(3, 88)
        self.selection_table.setColumnWidth(4, 71)
        self.selection_table.setColumnWidth(5, 75)
        self.selection_table.setColumnWidth(6, 350)

    def _get_or_create_widget(self, widget_type: str, original_row_count: int, row: int, col: int):
        widget = None
        if row < original_row_count:
            widget = self.selection_table.cellWidget(row, col)
            if widget:
                return widget
        if widget_type == "QLabel":
            widget = QLabel()
            widget.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        elif widget_type == "QCheckBox":
            widget = QCheckBox()
            widget.setAttribute(QtCore.Qt.WidgetAttribute.WA_TransparentForMouseEvents)
            widget.setFocusPolicy(QtCore.Qt.FocusPolicy.NoFocus)
            widget.setStyleSheet("QCheckBox { margin-right: 14px; margin-left: 14px; }")
        self.selection_table.setCellWidget(row, col, widget)
        return widget

    def update_table(self, current_selection: dict[int, int], visible_row_count: int, links: dict[int, MwLink]):
        self.selection_table.setUpdatesEnabled(False)

        if self.selection_table.rowCount() < visible_row_count:
            self.selection_table.setRowCount(visible_row_count)
            original_row_count = self.selection_table.rowCount()
        else:
            original_row_count = visible_row_count

        row = 0
        for link_id in current_selection:
            if current_selection[link_id] == 0:
                continue

            self.selection_table.showRow(row)

            # columns: 0 = ID, 1 = channel 1, 2 = channel 2, 3 = technology, 4 = band, 5 = length, 6 = name

            id_label = self._get_or_create_widget("QLabel", original_row_count, row, 0)
            id_label.setText(str(link_id))

            tech_label = self._get_or_create_widget("QLabel", original_row_count, row, 3)
            tech_label.setText(links[link_id].tech)

            band_label = self._get_or_create_widget("QLabel", original_row_count, row, 4)
            band_label.setText("{:.0f}".format(links[link_id].freq_a / 1000))

            length_label = self._get_or_create_widget("QLabel", original_row_count, row, 5)
            length_label.setText("{:.2f}".format(links[link_id].distance))

            name_label = self._get_or_create_widget("QLabel", original_row_count, row, 6)
            name_label.setText(links[link_id].name)

            channel_1_box = self._get_or_create_widget("QCheckBox", original_row_count, row, 1)
            channel_2_box = self._get_or_create_widget("QCheckBox", original_row_count, row, 2)

            if current_selection[link_id] == 1:
                channel_1_box.setChecked(True)
                channel_2_box.setChecked(False)
            elif current_selection[link_id] == 2:
                channel_1_box.setChecked(False)
                channel_2_box.setChecked(True)
            elif current_selection[link_id] == 3:
                channel_1_box.setChecked(True)
                channel_2_box.setChecked(True)

            row += 1

        # hide the rest of the rows
        for r in range(row, self.selection_table.rowCount()):
            self.selection_table.hideRow(r)

        self.selection_table.setUpdatesEnabled(True)


class ErrorBox(QMessageBox):
    """In case of fatal error, show this box."""
    def __init__(self, box_title: str = "Error", box_error: str = "Unknown error occurred."):
        """
        Initialize the error box.

        :param box_title: title of the box
        :param box_error: error message
        """
        super().__init__()

        self.setIcon(QMessageBox.Icon.Critical)
        self.setWindowTitle(box_title)
        self.setWindowIcon(QIcon("./app/gui/icons/app_96x96.png"))
        self.setText(box_error)

        self.setStandardButtons(QMessageBox.StandardButton.Ok)
        self.setDefaultButton(QMessageBox.StandardButton.Ok)

        self.show()
