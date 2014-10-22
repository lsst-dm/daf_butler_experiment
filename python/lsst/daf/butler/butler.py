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
    """
    A Butler manages the persistence and retrieval of datasets in
    repositories for Tasks.

    A dataset is any data item that makes sense to persist or retrieve.  This
    can be as small as a single bit or as large as an image or a catalog of
    measurements.

    A repository is a collection of datasets of different types.  Each dataset
    type may be stored in a different physical location, which could be in a
    filesystem, in network-based storage, or in a database.

    One repository managed by a Butler is an output repository and is both
    readable and writable.  Other input read-only repositories may be
    attached; they are searched for datasets that are not found in the output
    repository.  The input repositories may themselves have other input
    repositories attached, forming a directed graph.  This graph is searched
    depth-first.
    """

    def __init__(self, outputRepo, inputRepos=None):
        """Construct a Butler to manage an output (read/write) repository,
        attaching zero or more input (read-only) repositories."""

        self.mapper = Mapper.create(outputRepo, inputRepos)
        self.registryPath = self.mapper.registryPath
        self.provenance = []

    def get(self, datasetType, dataId={}, **kwArgs):
        """Retrieve a dataset."""

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
        """Persist a dataset."""

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
        """Return the list of keys understood by the Butler for a given
        dataset type or all dataset types if datasetType=None (default)."""

        datasetType = self._handleAlias(datasetType)
        return self.mapper.getKeys(datasetType)

    def getDatasetTypes(self):
        """Return the list of known dataset types."""

        return self.mapper.getDatasetTypes()

    def createDatasetType(self, datasetType, datasetClass, pathTemplate, **kwargs):
        """Create a new dataset type based on an existing dataset class."""

        datasetType = self._handleAlias(datasetType)
        return self.mapper.createDatasetType(datasetType, datasetClass, **kwargs)

    def getRefSet(self, datasetType, partialDataId={}, **kwargs):
        """Return the list of references to datasets of a given dataset type
        that match a partial data id."""

        datasetType = self._handleAlias(datasetType)
        partialDataId = self._makeDataId(partialDataId, **kwArgs)
        return [DataRef(self, dataId)
                for dataId in
                self.mapper.listDatasets(self, datasetType, partialDataId)]

    def defineAlias(self, alias, datasetType):
        """Define a dataset type alias.  This dataset type can be used in any
        other Butler call by specifying "@" plus the alias name."""

        if alias in self.aliases:
            log.warn("Overwriting existing dataset type alias {}: "
                    "old = {}, new = {}".format(
                        self.aliases[alias], datasetType))
        self.aliases[alias] = datasetType

    def recordProvenance(self, op, datasetType, dataId, locationList):
        """Record provenance information."""

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

