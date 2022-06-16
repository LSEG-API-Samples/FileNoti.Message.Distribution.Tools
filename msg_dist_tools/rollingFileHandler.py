from logging.handlers import RotatingFileHandler
import itertools
import os


class RollingFileHandler(RotatingFileHandler):
    # override
    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        # my code starts here
        for i in itertools.count(1):
            nextName = "%s.%d" % (self.baseFilename, i)
            #print(nextName)
            if not os.path.exists(nextName):
                self.rotate(self.baseFilename, nextName)
                break
        # my code ends here
        if not self.delay:
            self.stream = self._open()