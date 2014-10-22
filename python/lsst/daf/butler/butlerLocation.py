import importlib

class ButlerLocation(object):
    def __init__(self, path, storage, dataId):
        """Create a ButlerLocation object."""

        self.url = path
        components = storage.rsplit(".", 2)
        if len(components) < 3:
            raise RuntimeError("No module or class for "
                    "storage {}".format(storage))
        module = importlib.import_module(components[0])
        if not hasattr(module, components[1]):
            raise RuntimeError("No such class {} for storage {}".format(
                components[1], storage))
        cls = getattr(module, components[1])
        if not hasattr(cls, components[2]):
            raise RuntimeError("No such method {} for storage {}".format(
                components[2], storage))
        self.storage = getattr(cls, components[2])
        self.dataId = dataId

    def get(self, predecessor):
        return self.storage(self.url, self.dataId, predecessor)

    def put(self, obj):
        return self.storage(obj, self.url, self.dataId)
