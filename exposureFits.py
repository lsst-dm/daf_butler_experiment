class Exposure(object):
    def __init__(self, path):
        self.path = path

    def __str__(self):
        return "Exposure({})".format(self.path)

class ExposureFits(object):
    @staticmethod
    def get(url, dataId, predecessor):
        return Exposure(url)
