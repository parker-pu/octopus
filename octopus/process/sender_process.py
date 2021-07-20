from multiprocessing import Process


class SenderProcess(Process):
    """
    sender process
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
