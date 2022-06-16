import signal
import time
import sys


class GracefulExiter():

    def __init__(self):
        self.state = False
        signal.signal(signal.SIGINT, self.change_state)

    def change_state(self, signum, frame):
        print("exit flag set to True (repeat to exit now)")
        #signal.signal(signal.SIGINT, signal.SIG_DFL)
        #self.state = True
        sys.exit(0)

    def exit(self):
        return self.state