import logging as log
import os

from dataRef import DataRef
from dbLock import DbLock
from mapper import Mapper

# One butler per task
# One mapper per repo, customized for camera
  # config can specify parent repos
# Datasets loaded from first repo in search order
# One registry per repo

# Dataset classes specify readers/writers, template templates
# Subsets specify additional dataId keys, query/fragment identifiers?
# Describe existing datasets
# Describe output datasets
  # obs-specific, obs-generic
  # dataset class
  # path template
  # dataid keys



class Butler(object):

    def __init__(self, outputRepo, inputRepos=None):
        self.mapper = Mapper.create(outputRepo, inputRepos)
        self.registryPath = self.mapper.registryPath
        self.provenance = []

    def get(self, datasetType, dataId={}, **kwArgs):
        datasetType = self._handleAlias(datasetType)
        dataId = self._makeDataId(dataId, **kwArgs)
        locationList = []
        locationList = self.mapper.map(datasetType, dataId, False)
        if len(locationList) == 0:
            _fatal(RuntimeError,
                    "Unrecognized dataset type {}".format(datasetType))
        obj = None
        for location in locationList:
            obj = location.get(obj)
        self.recordProvenance("get", datasetType, dataId, locationList)
        return obj

    def put(self, obj, datasetType, dataId={}, **kwArgs):
        datasetType = self._handleAlias(datasetType)
        dataId = self._makeDataId(dataId, **kwArgs)
        locationList = self.mapper.map(datasetType, dataId, True)
        if len(locationList) == 0:
            _fatal(RuntimeError,
                    "Unrecognized dataset type {}".format(datasetType))
        with self._lock(datasetType, dataId):
            if self.mapper.datasetExists(datasetType, dataId):
                if self.get(datasetType, dataId) == obj:
                    return
                _fatal(RuntimeError, "Attempt to overwrite dataset "
                        "at {} (type={}, dataId={}) "
                        "with different content: {}".format(
                            location, datasetType, dataId, obj))
            for location in locationList:
                location.put(obj)
            self.recordProvenance("put", datasetType, dataId, locationList)

    def getKeys(self, datasetType=None):
        datasetType = self._handleAlias(datasetType)
        return self.mapper.getKeys(datasetType)

    def getDatasetTypes(self):
        return self.mapper.getDatasetTypes()

    def createDatasetType(self, datasetType, datasetClass, pathTemplate, **kwargs):
        datasetType = self._handleAlias(datasetType)
        return self.mapper.createDatasetType(datasetType, datasetClass, **kwargs)

    def getRefSet(self, datasetType, partialDataId={}, **kwargs):
        datasetType = self._handleAlias(datasetType)
        partialDataId = self._makeDataId(partialDataId, **kwArgs)
        return [DataRef(self, dataId)
                for dataId in
                self.mapper.listDatasets(self, datasetType, partialDataId)]

    def defineAlias(self, alias, datasetType):
        if alias in self.aliases:
            log.warn("Overwriting existing dataset type alias {}: "
                    "old = {}, new = {}".format(
                        self.aliases[alias], datasetType))
        self.aliases[alias] = datasetType

    def recordProvenance(self, op, datasetType, dataId, locationList):
        log.info("Provenance: {} {} {} {}".format(
            op, datasetType, dataId, locationList))
        self.provenance.append((op, datasetType, dataId, [repr(location) for
            location in locationList]))


###############################################################################

    def _makeDataId(self, dataId, **kwArgs):
        newDataId = dataId.copy()
        newDataId.update(kwArgs)
        return newDataId

    def _handleAlias(self, datasetType):
        if datasetType.startswith("@"):
            alias = datasetType[1:]
            if alias not in self.aliases:
                _fatal(KeyError,
                        "Undefined dataset type alias: {}".format(datasetType))
            return self.aliases[alias]
        return datasetType

    def _lock(self, datasetType, dataId):
        return DbLock(self.registryPath).lock(datasetType + ":" + repr(dataId))

###############################################################################

def _fatal(exception, message):
    log.fatal(message)
    raise exception(message)

