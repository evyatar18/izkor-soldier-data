class timer:
    from time import time as current_time

    def __init__(self):
        self.started = False
        self.start_time = 0
        self.stop_time = 0

    def reset(self):
        self.__init__()

    def start(self):
        self.start_time = self.current_time()
        self.started = True

    def stop(self):
        self.stop_time = self.current_time()
        self.started = False

    def get_value(self):
        if self.started:
            return self.current_time() - self.start_time

        else:
            return self.stop_time - self.start_time

    def print(self, start_msg = ""):
        print(start_msg, self.get_value(), "seconds passed.")
