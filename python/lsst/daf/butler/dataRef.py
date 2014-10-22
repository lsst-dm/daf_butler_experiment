class DataRef(dict):
    def __init__(self, butler, **kwargs):
        super(DataId, self).__init__(**kwargs)
        self.butler = butler
