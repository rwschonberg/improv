import sys
#sys.path.append('src')
#print(sys.path)
from PyQt5 import QtGui,QtCore
from visual import rasp_ui
from nexus.nexus import Nexus
import numpy as np
import pylab
import time
import pyqtgraph
from threading import Thread
from multiprocessing import Process
from PyQt5.QtWidgets import QMessageBox, QFileDialog
from process.process import CaimanProcessor as cp

import logging; logger = logging.getLogger(__name__)


class FrontEnd(QtGui.QMainWindow, rasp_ui.Ui_MainWindow):

    def __init__(self, parent=None):
    
        pyqtgraph.setConfigOption('background', 'w') #before loading widget
        super(FrontEnd, self).__init__(parent)
        
        self.setupUi(self)
        self.rawplot.ui.histogram.hide()
        self.rawplot.ui.roiBtn.hide()
        self.rawplot.ui.menuBtn.hide()
        self.checkBox.setChecked(True)
        
        self.nexus = Nexus('NeuralNexus')
        self.nexus.createNexus()

        #temp
        #self.proc = cp.setupProcess(self.nexus.Processor, 'params_dict')
        
        self.nexus.setupProcessor()

        self.pushButton_3.clicked.connect(_call(self._runProcess))
        self.pushButton_3.clicked.connect(_call(self.update))
        self.pushButton.clicked.connect(_call(self._loadParams))
        self.checkBox.stateChanged.connect(self.update)
    
    
    def _loadParams(self):
        ''' Button event to load parameters from file
            File location determined from user input
            Throws FileNotFound error
        '''
        fname = QFileDialog.getOpenFileName(self, 'Open file', '/home')
            #TODO: make default home folder system-independent
        try:
            self.nexus.loadTweak(fname[0])
        except FileNotFoundError as e:
            logger.error('File not found {}'.format(e))
            raise FileNotFoundError
    
    
    def _runProcess(self):
        #fnames = self.proc.client.get('params_dict')['fnames'] #CHANGEME
        #output = 'outputEstimates'
        self.t = Thread(target=self.nexus.runProcessor)
        self.t.daemon = True
        self.t.start()

        #TODO: grey out button until self.t is done, but allow other buttons to be active

        #self.p = Process(target=self.nexus.runProcessor)
        #self.p.start()
        #self.p.join()

        #cp.runProcess(self.proc, fnames, output) #TODO: need flag for multiple updates...


    def update(self):

        #plot traces
        Y = None
        try:
            self.ests = self.nexus.getEstimates()
            (X, Y) = self.nexus.getPlotEst()
        except Exception as e:
            logger.info('output does not yet exist. error: {}'.format(e))

        if(Y is not None):
            pen=pyqtgraph.mkPen(width=2)
            self.grplot.plot(X, Y,pen=pen,clear=True)

        #plot video
        image = None
        try:
            image = self.nexus.getPlotRaw()
        except Exception as e:
            logger.error('Oh no {0}'.format(e))

        if image is not None:
            self.rawplot.setImage(image)

        #re-update
        if self.checkBox.isChecked():
            QtCore.QTimer.singleShot(5, self.update)


    def closeEvent(self, event):

        confirm = QMessageBox.question(self, 'Message', 'Quit without saving?',
                    QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if confirm == QMessageBox.Yes:
            self.nexus.destroyNexus()
            event.accept()
        else: event.ignore()


def _call(fnc, *args, **kwargs):
    ''' Call handler for (external) events
    '''
    
    def _callback():
        return fnc(*args, **kwargs)
    return _callback


if __name__=="__main__":
    app = QtGui.QApplication(sys.argv)
    rasp = FrontEnd()
    rasp.show()
    #rasp.update() #start with something in plot
    app.exec_()
