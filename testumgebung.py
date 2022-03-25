import logging
from PyQt5.QtCore import QObject, QThread, pyqtSignal, pyqtSlot
import sys
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from t_fileparser import FileParser
from t_publishData import MqttPublisher
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

"""
construct SpecimenDataFrame:
The Specimen Dataframe contains the relevant Data
The contained Data in this Dataframe is of Type String
The order of the Elements is to be respected 0 to 6]
The Names are taken from the SpecimenNameFrame
"""

SpecimenNameList = ["sample_id", "station_id", "timestamp"]
SpecimenDataList = [None, "Zwick", None]
SpecimenDataFrame = [SpecimenNameList, SpecimenDataList]

# TODO: config File
broker = "192.168.192.21"
port = 1883
username = "Detact"
passkey = "Detact#1234"
BaseTopic = "probekoerper"
Publish = False

path = ""

def reset_SpecimenDataFrame():

    global SpecimenDataFrame

    SpecimenNameList = ["sample_id", "station_id", "timestamp"]
    SpecimenDataList = [None, "Zwick", None]

    SpecimenDataFrame = []
    SpecimenDataFrame = [SpecimenNameList, SpecimenDataList]


def process_excel_sheet(excel_path):

    print(excel_path)


    #try:
    # TODO: make Komment refering to the driffrent sheets in the standard export

    df = pd.read_excel(excel_path, 3)
    print(df)
    logging.info("Excel sheet " + excel_path + "has successfully imported as pandas Data Frame")

    SpecimenDataFrame[1][0] = str(df.columns[0])

    # automatically read collum Values

    for col in df.columns:
        # get channels and ad the corresponding unit to the Name
        channel_name = str(df.iloc[0, df.columns.get_loc(col)])
        channel_unit = str(df.iloc[1, df.columns.get_loc(col)])
        channel_name_and_unit = channel_name + " [" + channel_unit + "]"

        SpecimenDataFrame[0].append(channel_name_and_unit)

        # get channel Value
        # SpecimenDataFrame[1].append(str(df.iloc[2, df.columns.get_loc(col)]))

    print("first header:")
    print(SpecimenDataFrame[0])

    # get reference Time for building timestamps
    start_time = datetime.now()

    # init MQTT Client
    Client = MqttPublisher("Zwick " + "Publish", broker, port, username, passkey)

    # start counting in line 2
    for line in range(2, len(df)):

        for col in df.columns:
            # get channel Value
            SpecimenDataFrame[1].append(df.iloc[line, df.columns.get_loc(col)])

            # produce a timestamp
            if "Prüfzeit" in SpecimenDataFrame[0][df.columns.get_loc(col) + 3]:
                delta_t = float(df.iloc[line, df.columns.get_loc(col)])

                if str(df.iloc[1, df.columns.get_loc(col)]) == "s":
                    SpecimenDataFrame[1][2] = str(start_time + timedelta(days=0, seconds=delta_t))

                if str(df.iloc[1, df.columns.get_loc(col)]) == "min":

                    delta_t = delta_t/60
                    SpecimenDataFrame[1][2] = str(start_time + timedelta(days=0, seconds=delta_t))

        # TODO: publish data on mqtt

        if str(SpecimenDataFrame[1][0]) not in ["", " ", "none", "None", "False", "false"]:

            print(build_json(SpecimenDataFrame))
            Client.publish(BaseTopic, build_json(SpecimenDataFrame))
            time.sleep(0.1)


        # shorten the SpecimenDataFrame so it can be overwritten in the next iteration
        SpecimenDataFrame[1] = SpecimenDataFrame[1][:-len(df.columns) or None]


    #except Exception:
     #   logging.error("Error while importing Execl sheet " + excel_path)

    reset_SpecimenDataFrame()


def timestamp():
    current_time = datetime.datetime.now()
    time_stamp = current_time.strftime("%d-%m-%y_%H-%M-%S")
    return_time = "%s" % time_stamp
    return time_stamp
    # https://www.programiz.com/python-programming/datetime/timestamp-datetime


def build_json(dataframe):
    """
    :param dataframe: takes the 2D Array SpecimenDataframe
    :return: json string build output of the provided Dataframe
    """
    data_set = {}
    json_dump = ""
    dataframe_length = int(len(dataframe[1]))

    # deconstruct specimenDataFrame and pack value pairs into .json
    if len(dataframe[0]) == len(dataframe[1]):
        for x in range(0, dataframe_length):
            if dataframe[1][x] not in ["", " ", None] and "Prüfzeit" not in dataframe[0][x]:
                key = str(dataframe[0][x])
                value = (dataframe[1][x])
                data_set[key] = value

            json_dump = json.dumps(data_set)
    else:
        logging.error("Error while transforming list into json String")

    return json_dump


class PublishData(QThread):
    """
    Thread class for continuously publishing the updated specimenDataframe
    to the MQTT Broker
    """

    @pyqtSlot()
    def run(self):
        self.Client = MqttPublisher("Zwick " + "Publish", broker, port, username, passkey)
        print("init")

        while True:
            if str(SpecimenDataFrame[1][0]) not in ["", " ", "none", "None", "False", "false"]:
                if Publish:
                    self.Client.publish(BaseTopic, build_json(SpecimenDataFrame))
                    Publish = False


class ConsoleWorkerPublish(QObject):
    """
    worker Object to  for continuously publishing the updated specimenDataframe
    see: class PublishData(QThread)
    """
    def __init__(self):
        super().__init__()
        self.Communicator = PublishData()

    def start_communication_thread(self):
        self.Communicator.start()

    def stop_communication_thread(self):
        self.Communicator.exit()


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
        # Set the layout
        layout = QVBoxLayout()
        layout.addWidget(self.clicksLabel)
        layout.addWidget(self.countBtn)
        layout.addWidget(self.stepLabel)
        layout.addWidget(self.longRunningBtn)
        self.centralWidget.setLayout(layout)

    def runTHREAD(self):
        # init of the Tread class Objekt
        self.fp = FileParser(r"C:\Users\ennoh\Documents\Test")

        # run Thread Object
        self.fp.start()

        # connect signals to worker Methods
        self.fp.finished.connect(self.fp.quit)
        self.fp.finished.connect(self.fp.deleteLater)
        self.fp.new_file_signal.connect(self.handle_new_file_signal)
        self.fp.File_Path_Signal.connect(self.handle_File_Path_Signal)

        self.stepLabel.setText("Thread gestartet")
        self.stepLabel.repaint()

    def stopTHREAD(self):

        try:
            self.fp.exit()
        except Exception:
            logging.error("the thread could not be stopped (was it running?)")

    # Worker Methods reacting to signals emitted from the Tread Class
    def handle_new_file_signal(self, filename):
        print("File detected: filename: " + filename)

    def handle_File_Path_Signal(self, file_location):
        print("received File_Path_Signal")
        print("Path: " + str(file_location))
        self.clicksLabel.setText("Detected file, processing, please wait...")
        self.clicksLabel.repaint()
        time.sleep(1)
        process_excel_sheet(file_location)
        self.clicksLabel.setText("Data has been published.")
        self.clicksLabel.repaint()


# run application
app = QApplication(sys.argv)
win = Window()
win.show()
sys.exit(app.exec())
