from PyQt6 import uic
from PyQt6 import QtCore
from PyQt6.QtWidgets import QDialog, QTableWidget, QDialogButtonBox, QLabel, QCheckBox, QGridLayout, QWidget


class SelectionDialog(QDialog):
    def __init__(self, parent=None, selection: dict = None, links: dict = None):
        super().__init__(parent)
        self.selection = selection

        # load UI definition from Qt XML file
        uic.loadUi("./gui/SelectionDialog.ui", self)

        self.table = self.findChild(QTableWidget, "tableSelection")
        self.butt_box = self.findChild(QDialogButtonBox, "buttonBox")

        # declare dict containing channels checkboxes for each link
        self.link_checks = {}

        self.butt_box.accepted.connect(self.dialog_confirmed)

        # style columns
        self.table.setColumnWidth(0, 40)
        self.table.setColumnWidth(1, 86)
        self.table.setColumnWidth(2, 86)
        self.table.setColumnWidth(3, 312)
        self.table.setColumnWidth(4, 71)
        self.table.setColumnWidth(5, 121)
        self.table.setColumnWidth(6, 121)
        self.table.setColumnWidth(7, 71)
        self.table.setColumnWidth(8, 88)
        self.table.setColumnWidth(9, 88)
        self.table.setColumnWidth(10, 46)

        # ////// Fill table: \\\\\\

        self.table.setRowCount(len(selection))

        # columns: 0 = ID, 1 = channel 1, 2 = channel 2, 3 = name, 4 = tech, 5 = freq A, 6 = freq B, 7 = polarization,
        # 8 = IP A, 9 = IP B, 10 = length
        for row, link_id in enumerate(self.selection):
            id_label = QLabel(str(link_id))
            id_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            name_label = QLabel(links[link_id].name)
            tech_label = QLabel(links[link_id].tech)
            tech_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            freq_a_label = QLabel(str(links[link_id].freq_a))
            freq_a_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            freq_b_label = QLabel(str(links[link_id].freq_b))
            freq_b_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            pol_label = QLabel(links[link_id].polarization)
            pol_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            ip_a_label = QLabel(links[link_id].ip_a)
            ip_a_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            ip_b_label = QLabel(links[link_id].ip_b)
            ip_b_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            length_label = QLabel("{:.2f}".format(links[link_id].distance))
            length_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

            channel_1 = QCheckBox()
            channel_2 = QCheckBox()

            if self.selection[link_id] == 1:
                channel_1.setChecked(True)
                channel_2.setChecked(False)
            elif self.selection[link_id] == 2:
                channel_1.setChecked(False)
                channel_2.setChecked(True)
            elif self.selection[link_id] == 3:
                channel_1.setChecked(True)
                channel_2.setChecked(True)
            elif self.selection[link_id] == 0:
                channel_1.setChecked(False)
                channel_2.setChecked(False)

            self.link_checks[link_id] = {'a': channel_1, 'b': channel_2}

            # Qt TableWidget formatting weirdness:

            channel_1_box = QGridLayout()
            channel_1_box.addWidget(self.link_checks[link_id]['a'], 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignCenter)
            channel_1_box.setContentsMargins(0, 0, 0, 0)
            channel_1_box_box = QWidget()
            channel_1_box_box.setLayout(channel_1_box)

            channel_2_box = QGridLayout()
            channel_2_box.addWidget(self.link_checks[link_id]['b'], 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignCenter)
            channel_2_box.setContentsMargins(0, 0, 0, 0)
            channel_2_box_box = QWidget()
            channel_2_box_box.setLayout(channel_2_box)

            self.table.setCellWidget(row, 0, id_label)
            self.table.setCellWidget(row, 1, channel_1_box_box)
            self.table.setCellWidget(row, 2, channel_2_box_box)
            self.table.setCellWidget(row, 3, name_label)
            self.table.setCellWidget(row, 4, tech_label)
            self.table.setCellWidget(row, 5, freq_a_label)
            self.table.setCellWidget(row, 6, freq_b_label)
            self.table.setCellWidget(row, 7, pol_label)
            self.table.setCellWidget(row, 8, ip_a_label)
            self.table.setCellWidget(row, 9, ip_b_label)
            self.table.setCellWidget(row, 10, length_label)

    # when ok is fired
    def dialog_confirmed(self):
        for link_id in self.link_checks:
            is_a = self.link_checks[link_id]['a'].isChecked()
            is_b = self.link_checks[link_id]['b'].isChecked()

            if is_a and is_b:
                self.selection[link_id] = 3
            elif is_a and not is_b:
                self.selection[link_id] = 1
            elif not is_a and is_b:
                self.selection[link_id] = 2
            else:
                self.selection[link_id] = 0

        # accept dialog
        self.accept()
