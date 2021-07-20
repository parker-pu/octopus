from multiprocessing import Process


class ReadProcess(Process):
    """
    Read process
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
