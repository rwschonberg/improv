# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'bwrap_ui_demo.ui'
#
# Created by: PyQt5 UI code generator 5.15.7
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_Demo(object):
    def setupUi(self, Demo):
        Demo.setObjectName("Demo")
        Demo.resize(600, 485)
        self.groupBox = QtWidgets.QGroupBox(Demo)
        self.groupBox.setEnabled(True)
        self.groupBox.setGeometry(QtCore.QRect(40, 82, 111, 101))
        self.groupBox.setObjectName("groupBox")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout(self.groupBox)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.pushButton_4 = QtWidgets.QPushButton(self.groupBox)
        self.pushButton_4.setObjectName("pushButton_4")
        self.verticalLayout_2.addWidget(self.pushButton_4)
        self.pushButton_3 = QtWidgets.QPushButton(self.groupBox)
        self.pushButton_3.setObjectName("pushButton_3")
        self.verticalLayout_2.addWidget(self.pushButton_3)
        self.label = QtWidgets.QLabel(Demo)
        self.label.setGeometry(QtCore.QRect(180, 52, 111, 20))
        self.label.setAlignment(QtCore.Qt.AlignLeading|QtCore.Qt.AlignLeft|QtCore.Qt.AlignVCenter)
        self.label.setObjectName("label")
        self.groupBox_3 = QtWidgets.QGroupBox(Demo)
        self.groupBox_3.setEnabled(True)
        self.groupBox_3.setGeometry(QtCore.QRect(40, 190, 111, 83))
        self.groupBox_3.setObjectName("groupBox_3")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.groupBox_3)
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.checkBox = QtWidgets.QCheckBox(self.groupBox_3)
        self.checkBox.setObjectName("checkBox")
        self.verticalLayout_3.addWidget(self.checkBox)
        self.checkBox_2 = QtWidgets.QCheckBox(self.groupBox_3)
        self.checkBox_2.setObjectName("checkBox_2")
        self.verticalLayout_3.addWidget(self.checkBox_2)
        self.frame = QtWidgets.QFrame(Demo)
        self.frame.setGeometry(QtCore.QRect(180, 82, 361, 191))
        self.frame.setStyleSheet("background-color: rgb(97, 141, 255);\n"
"background-color: rgb(196, 229, 255);")
        self.frame.setObjectName("frame")

        self.retranslateUi(Demo)
        QtCore.QMetaObject.connectSlotsByName(Demo)

    def retranslateUi(self, Demo):
        _translate = QtCore.QCoreApplication.translate
        Demo.setWindowTitle(_translate("Demo", "Demo"))
        self.groupBox.setTitle(_translate("Demo", "Operations"))
        self.pushButton_4.setText(_translate("Demo", "Setup"))
        self.pushButton_3.setText(_translate("Demo", "Run"))
        self.label.setText(_translate("Demo", "Bubblewrap"))
        self.groupBox_3.setTitle(_translate("Demo", "Plot style"))
        self.checkBox.setText(_translate("Demo", "Lineplot"))
        self.checkBox_2.setText(_translate("Demo", "Scatterplot"))

if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    Demo = QtWidgets.QWidget()
    ui = Ui_Demo()
    ui.setupUi(Demo)
    Demo.show()
    sys.exit(app.exec_())

