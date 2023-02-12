class KlineThreadControl:
    thread_count = 0
    current_finished_num = 0

    def __init__(self, thread_count, current_finished_num):
        self.thread_count = thread_count
        self.current_finished_num = current_finished_num