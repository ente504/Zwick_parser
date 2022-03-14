from PyQt5.QtCore import QObject, QThread, pyqtSignal, pyqtSlot
import sys
from time import sleep
from t_fileparser import FileParser
from PyQt5.QtGui import QIcon, QPixmap
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QPushButton,
    QVBoxLayout,
    QWidget)

"""
simple QT test enviromet for diagnosing purposes only!

"""

path = ""


class Worker(QObject):
    """
    only needed for moving a method (defined inside the main program)
    into a thread
    """

    global path
    finished = pyqtSignal()

    def run(self):
        self.fp = FileParser(r"C:\Users\Pruefer\Documents\Probenerfassungskamera\Test",
                             r"C:\Users\Pruefer\Documents\Probenerfassungskamera\Image_Data")
        self.fp.start()

        path = self.fp.Image_Path_Signal

    def stop(self):
        self.fp.clean_up_dirs(r"C:\Users\Pruefer\Documents\Probenerfassungskamera\Test",
                              r"C:\Users\Pruefer\Documents\Probenerfassungskamera\Image_Data")


class Window(QMainWindow):
    """
    GUI definition (pyqt)
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.clicksCount = 0
        self.setupUi()

    def setupUi(self):
        self.setWindowTitle("Testumgebung")
        self.resize(300, 500)
        self.centralWidget = QWidget()
        self.setCentralWidget(self.centralWidget)
        # Create and connect widgets
        self.clicksLabel = QLabel("-", self)
        self.clicksLabel.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.stepLabel = QLabel("-")
        self.stepLabel.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.countBtn = QPushButton("Stop Thread", self)
        self.countBtn.clicked.connect(self.stopTHREAD)
        self.longRunningBtn = QPushButton("Start Thread", self)
        self.longRunningBtn.clicked.connect(self.runTHREAD)
        # declare Image Label
        self.im = QPixmap("DMT_Logo.png")
        self.lable = QLabel()
        self.lable.setPixmap(self.im)
        self.lable.show()

        # Set the layout
        layout = QVBoxLayout()
        layout.addWidget(self.clicksLabel)
        layout.addWidget(self.countBtn)
        layout.addWidget(self.stepLabel)
        layout.addWidget(self.longRunningBtn)
        layout.addWidget(self.lable)
        self.centralWidget.setLayout(layout)

    def runTHREAD(self):
        # init of the Tread class Objekt
        self.stepLabel.setText("Thread initalisiert")
        self.fp = FileParser(r"C:\Users\ennoh\Documents\Test")

        # run Thread Objekt
        self.fp.start()
        self.stepLabel = QLabel("thread gestartet")


        # connect signals to worker Methods
        self.fp.finished.connect(self.fp.quit)
        self.fp.finished.connect(self.fp.deleteLater)
        self.fp.new_file_signal.connect(self.handle_new_file_signal)
        self.fp.File_Path_Signal.connect(self.handle_File_Path_Signal)

        self.stepLabel.setText("Thread gestartet")

    def stopTHREAD(self):
        self.fp.exit()

    # Worker Methods reacting to signals emitted from the Tread Class
    def handle_new_file_signal(self, filename):
        print("File detected: filename: " + filename)

    def handle_File_Path_Signal(self, file_location):
        print("received File_Path_Signal")
        print("Path: " + str(file_location))


# run application
app = QApplication(sys.argv)
win = Window()
win.show()
sys.exit(app.exec())
