import datetime
import os
import pprint
import sys


class Logger:
    def __init__(self, script_name=None, stdout=False):
        now = datetime.datetime.now()
        today = now.strftime('%Y-%m-%d')
        time = now.strftime('%I:%M %p')
        name = script_name or os.path.split(sys.argv[0])[1]
        self._log = []
        self._log.append("%s | %s:%s on %s" % (name, today, time, os.getenv('COMPUTERNAME')))
        self.stdout = stdout
        self.pp = pprint.PrettyPrinter(indent=1)

    def log(self, msg, stdout=False):
        """
        stores a log message and prints to stdout
        """
        self._log.append('{} | {}'.format(datetime.datetime.now().strftime('%I:%M %p'), msg))

        if stdout or self.stdout:
            print(msg)

    def log_error(self, msg):
        import traceback
        self.log(traceback.format_exc())

    def print_log(self):
        return self.pp.pformat(self._log)

    def save(self, folder, file_name=None):
        """
        writes the log to a
        """
        if not os.path.exists(folder):
            os.mkdir(folder)

        if file_name is None:
            file_name = datetime.datetime.now().strftime('%Y-%m-%d') + '.txt'

        with open(os.path.join(folder, file_name), mode='a') as f:
            for line in self._log:
                f.write(line + '\n')
