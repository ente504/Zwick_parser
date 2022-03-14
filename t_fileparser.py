from PyQt5.QtCore import QThread, QFileSystemWatcher, pyqtSignal, pyqtSlot
import os
import logging


class FileParser(QThread):

    # Signals
    new_file_signal = pyqtSignal(str, name="new_file_signal")
    File_Path_Signal = pyqtSignal(str, name="File_Path_Signal")

    def __init__(self, directory_to_watch):

        super().__init__()
        logging.info("initialising  of the file detection Thread")

        # init variables
        self.watchdog_path = []
        self.watchdog_path.append(directory_to_watch)
        self.path = os.path.normpath(self.watchdog_path[0])
        pathlist = [self.path]
        self.dir = os.listdir(self.path)
        self.dir2 = []
        self.delta = []
        self.directory_to_watch = directory_to_watch
        self.filename = ""
        self.file_location = ""

        # Slots, init of pyqt FileSytemWatcher
        self.qFSW = QFileSystemWatcher(self.watchdog_path)
        self.qFSW.directoryChanged.connect(self.directory_changed)

        logging.info("init of the file detection Thread has finished")

    @pyqtSlot(str)
    def directory_changed(self):

        """
        Function is to be called after a change in the monitored dict. is detected.
        It isolates the changed file, and checks if the detected file is an Excel file.

        this function emits the needed Signals to the Mainthread.
        """

        self.filename = ""
        self.file_location = ""
        self.delta = ""
        self.dir2 = os.listdir(self.path)
        self.delta = list(set(self.dir2) - set(self.dir))

        if self.delta:
            # isolation Filename and emit file detection Signal
            self.filename = str(self.delta) [2:-2]
            self.new_file_signal.emit(self.filename)
            logging.info("a new file has been detected")

            if self.filename.endswith((".xlsx", ".xls")):
                logging.info("the detected file is of type .xlsx or .xls")
                self.file_location = self.directory_to_watch + "\\" + self.filename
                self.File_Path_Signal.emit(self.file_location)

            # set actual dir to be the dir to compare to
            self.dir = self.dir2
