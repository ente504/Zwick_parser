# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# Zwick to Detact import Software
# Revision: @ente504
# 1.0: Initial version


import logging
from PyQt5.QtCore import QObject, QThread, pyqtSignal, pyqtSlot
import sys
import os
import pandas as pd
import time
import json
import configparser
from datetime import datetime, timedelta
from t_fileparser import FileParser
from t_publishData import MqttPublisher
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QPushButton,
    QVBoxLayout,
    QWidget)

# read from configuration file
CONFIG_FILE = "config.ini"
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

DeviceName = str(config["GENERAL"]["DeviceName"])
DirectoryToWatch = str(config["GENERAL"]["DirectoryToWatch"])
broker = str(config["MQTT"]["Broker"])
port = int(config["MQTT"]["Port"])
username = str(config["MQTT"]["UserName"])
passkey = str(config["MQTT"]["PassKey"])
BaseTopic = str(config["MQTT"]["BaseTopic"])
Excel_Sheet = int(config["GENERAL"]["ExcelSheet"])
Readme_Path = str(config["GENERAL"]["ReadmePath"])

# variable declarations
File_Location = ""
Publish = False
path = ""

"""
construct SpecimenDataFrame:
The Specimen Dataframe contains the relevant Data
The contained Data in this Dataframe is of Type String
The order of the Elements is to be respected 0 to 6]
The Names are taken from the SpecimenNameFrame
"""

SpecimenNameList = ["sample_id", "station_id", "timestamp"]
SpecimenDataList = [None, DeviceName, None]
SpecimenDataFrame = [SpecimenNameList, SpecimenDataList]


def reset_SpecimenDataFrame():

    global SpecimenDataFrame

    SpecimenNameList = ["sample_id", "station_id", "timestamp"]
    SpecimenDataList = [None, DeviceName, None]

    SpecimenDataFrame = []
    SpecimenDataFrame = [SpecimenNameList, SpecimenDataList]


def process_excel_sheet(excel_path, excel_sheet):
    """
    main working function. it reads all nessessary data from a given Excel Sheet, sorts it into the SpecimenDataframe
    and publishes the Data on MQTT to the Detact System.
    :param excel_path: Path there the Excel File is stored. Type: String
    :param excel_sheet: Number of the Excel Sheet witch contains the necessary non stationary Data Type: Integer
                        Starts at 0.
    """

    """
    read the non stationary data
    """

    df = pd.read_excel(excel_path, excel_sheet)
    logging.info("Excel sheet " + excel_path + "has successfully imported as pandas Data Frame")

    sample_id = str(df.columns[0])
    SpecimenDataFrame[1][0] = str(df.columns[0])

    # automatically read collum Values

    for col in df.columns:
        # get channels and ad the corresponding unit to the Name
        channel_name = str(df.iloc[0, df.columns.get_loc(col)])
        channel_unit = str(df.iloc[1, df.columns.get_loc(col)])
        channel_name_and_unit = channel_name + " [" + channel_unit + "]"
        SpecimenDataFrame[0].append(channel_name_and_unit)

    # get reference Time for building timestamps
    start_time = datetime.now()

    # init MQTT Client
    Client = MqttPublisher("Zwick " + "Publish", broker, port, username, passkey)

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

        # publish on MQTT
        if str(SpecimenDataFrame[1][0]) not in ["", " ", "none", "None", "False", "false"]:
            Client.publish(BaseTopic, build_json(SpecimenDataFrame))
            time.sleep(0.1)

        # shorten the SpecimenDataFrame so it can be overwritten in the next iteration
        SpecimenDataFrame[1] = SpecimenDataFrame[1][:-len(df.columns) or None]

    reset_SpecimenDataFrame()

    """ 
    read stationary Material parameters
    """

    df = pd.read_excel(excel_path, 1)
    SpecimenDataFrame[1][0] = sample_id
    SpecimenDataFrame[1][2] = str(start_time)
    logging.info("Excel sheet " + excel_path + "has successfully imported as pandas Data Frame")

    for col in range(2, len(df.columns)):
        value_name = str(df.columns[col])
        value_unit = str(df.iat[0, col])
        value_name_and_unit = value_name + " [" + value_unit + "]"
        value = df.iat[1, col]
        SpecimenDataFrame[0].append(value_name_and_unit)
        SpecimenDataFrame[1].append(value)
    # publish on MQTT
    if str(SpecimenDataFrame[1][0]) not in ["", " ", "none", "None", "False", "false"]:
        Client.publish(BaseTopic, build_json(SpecimenDataFrame))
        time.sleep(0.1)

    reset_SpecimenDataFrame()


def timestamp():
    """
    function produces an actual timestamp
    :return: timestamp as String
    """
    current_time = datetime.datetime.now()
    time_stamp = current_time.strftime("%d-%m-%y_%H-%M-%S")
    return_time = "%s" % time_stamp
    return time_stamp


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


class ConsoleWorkerExcel(QObject):
    """
    worker Object to strip Data from the Excel Sheet
    """
    finished = pyqtSignal()

    def run(self):
        """Long-running task."""
        process_excel_sheet(File_Location, Excel_Sheet)
        self.finished.emit()


class Window(QMainWindow):
    """
    GUI definition (pyqt)
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi()
        self.runTHREAD()

    def setupUi(self):
        """
        methode builds the pyqt GUI then the Window class is initialised
        """

        self.setWindowTitle("Detact Import Tool")
        self.resize(320, 200)
        self.centralWidget = QWidget()
        self.setCentralWidget(self.centralWidget)

        # Create and connect widgets
        self.Path_Label = QLabel("Directory to Watch: " + DirectoryToWatch, self)
        self.Path_Label.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.Broker_Label = QLabel("Broker: " + broker, self)
        self.Broker_Label.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.Port_Label = QLabel("Port: " + str(port), self)
        self.Port_Label.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.User_Label = QLabel("Username: " + username, self)
        self.User_Label.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.PW_Label = QLabel("Password: " + passkey, self)
        self.PW_Label.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.status_Label = QLabel("-")
        self.status_Label.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.Button_readme = QPushButton("open readme", self)
        self.Button_readme.clicked.connect(self.open_readme)
        self.Button_Exit = QPushButton("leave Application", self)
        self.Button_Exit.clicked.connect(self.stopTHREAD)

        # Set the layout
        layout = QVBoxLayout()
        layout.addWidget(self.Path_Label)
        layout.addWidget(self.Broker_Label)
        layout.addWidget(self.Port_Label)
        layout.addWidget(self.User_Label)
        layout.addWidget(self.PW_Label)
        layout.addWidget(self.status_Label)
        layout.addWidget(self.Button_readme)
        layout.addWidget(self.Button_Exit)

        self.centralWidget.setLayout(layout)

    def runTHREAD(self):
        """
        Start the File Detection Thread
        """

        # init of the Tread class Objekt
        self.fp = FileParser(DirectoryToWatch)

        # run Thread Object
        self.fp.start()

        # connect signals to worker Methods
        self.fp.finished.connect(self.fp.quit)
        self.fp.finished.connect(self.fp.deleteLater)
        self.fp.new_file_signal.connect(self.handle_new_file_signal)
        self.fp.File_Path_Signal.connect(self.handle_File_Path_Signal)
        self.status_Label.setStyleSheet("background-color: lightgreen")
        self.status_Label.setText("filemonitoring started")
        self.status_Label.repaint()

    def stopTHREAD(self):
        """
        kill the File Detection Thread
        """

        try:
            self.fp.exit()
        except Exception:
            logging.error("the thread could not be stopped (was it running?)")
        sys.exit()

    # Worker Methods reacting to signals emitted from the Tread Class
    def handle_new_file_signal(self, filename):
        logging.info("File detected: filename: " + filename)

    def handle_File_Path_Signal(self, file_location):
        """
        handles all necessary actions when a new filepath is available
        :param file_location: parameter is provided by the connected File_Path_Signal Type: String
        """

        global File_Location

        File_Location = file_location
        self.status_Label.setText("Detected file, processing, please wait...")
        self.status_Label.setStyleSheet("background-color: lightyellow")
        self.status_Label.repaint()

        # start thread for getting data from the Excel sheet
        self.thread = QThread()
        self.worker = ConsoleWorkerExcel()
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.start()
        self.thread.finished.connect(
            lambda: self.status_Label.setText("Data has been published.")
        )
        self.thread.finished.connect(
            lambda: self.status_Label.setStyleSheet("background-color: lightgreen")
        )
        self.status_Label.repaint()

    def open_readme(self):

        global Readme_Path
        try:
            os.startfile(Readme_Path)
        except Exception:
            logging.error("readme file was not found")


# run application
app = QApplication(sys.argv)
win = Window()
win.show()
sys.exit(app.exec())
