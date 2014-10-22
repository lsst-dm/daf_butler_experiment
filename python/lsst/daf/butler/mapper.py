import importlib
import logging as log
import os
import re
import sqlite3
import urlparse
import yaml

from butlerLocation import ButlerLocation

class Mapper(object):

    _mapperCache = {}

    @staticmethod
    def create(repoUrl, inputRepos=None):
        if repoUrl not in Mapper._mapperCache or inputRepos is not None:
            parseResult = urlparse.urlparse(repoUrl, scheme="file")
            if parseResult.scheme == "file":
                config = _readPathConfig(parseResult.path)
            elif parseResult.scheme == "sqlite":
                config = _readSqliteConfig(parseResult.path)
            else:
                _fatal(ValueError, "Unknown scheme {} for "
                        "repository URL {}".format(parseResult.scheme, repoUrl)) 
            if 'parent' not in config:
                config['parents'] = []
            if inputRepos is not None:
                for parent in inputRepos:
                    if parent not in config['parents']:
                        config['parents'].append(parent)

            Mapper._mapperCache[repoUrl] = Mapper._createFromConfig(
                    config, repoUrl)
        return Mapper._mapperCache[repoUrl]

    @staticmethod
    def _createFromConfig(config, source):
        if 'mapper' in config:
            mapperClassName = config['mapper']
        elif len(config['parents']) > 0:
            mapperClassName = Mapper.create(
                    config['parents'][0]).config['mapper']
        else:
            raise RuntimeError("No mapper class in mapper configuration "
                    "from {}".format(source))
        components = mapperClassName.rsplit(".", 1)
        if len(components) < 2:
            raise RuntimeError("No module name for mapper class {} "
                    "from {}".format(mapperClassName, source))
        module = importlib.import_module(components[0])
        if not hasattr(module, components[1]):
            raise RuntimeError("No such class {} for mapper class {} "
                    "from {}".format(components[1], mapperClassName, source))
        cls = getattr(module, components[1])
        return cls(config, source)

    def __init__(self, config, source):
        self.pickleArgs = (config, source)
        self.config = config
        self.source = source
        self.keyCache = {True: {}, False: {}}
        self.parents = [Mapper.create(parent) for parent in config['parents']]
        self.registryPath = os.path.join(config['repoPath'], "_butler.sqlite3")
        if 'mapper' not in self.config:
            self.config['mapper'] = type(self).__module__ + "." + type(self).__name__

        if "datasets" not in self.config:
            self.config["datasets"] = {}
        for datasetType in self.config["datasets"].keys():
            datasetConfig = self.config["datasets"][datasetType]
            if "datasetClass" not in datasetConfig:
                raise RuntimeError("No dataset class configured for "
                        "dataset type {} in mapper "
                        "from {}".format(datasetType, source))
            datasetClass = self.config["datasets"][datasetType]["datasetClass"]
            if not self.hasConfig('classes', datasetClass):
                raise RuntimeError("Unknown dataset class {} for "
                        "dataset type {} in mapper "
                        "from {}".format(datasetClass, datasetType, source))
            if "urls" not in datasetConfig:
                raise RuntimeError("No URL templates configured for "
                        "dataset type {} in mapper "
                        "from {}".format(datasetType, source))

        if "registryUrl" not in self.config:
            self.config["registryUrl"] = os.path.join(
                    self.config["repoPath"], "_butler.sqlite3")
        try:
            conn = sqlite3.connect(self.config["registryUrl"])
            conn.execute("CREATE TABLE IF NOT EXISTS _config (yaml TEXT)")
            conn.execute("DELETE FROM _config")
            yamlConfig = yaml.dump(self.config)
            conn.execute("INSERT INTO _config (yaml) VALUES (?)", [yamlConfig])
            conn.commit()
        finally:
            conn.close()

    def hasConfig(self, *args):
        """Search the mapper's config and its parents' configs for keys."""
        config = self.config
        found = True
        for key in args:
            if key in config:
                config = config[key]
            else:
                found = False
                break
        if found:
            return True
        for parent in self.parents:
            if parent.hasConfig(*args):
                return True
        return False

    # For pickling
    def __getstate__(self):
        return self.pickleArgs

    def __setstate__(self, state):
        self.__init__(state[0], state[1])


    def map(self, datasetType, dataId, forWrite):
        datasetConfig, datasetClass, classConfig, urlTemplates = \
                self._parseDatasetConfig(datasetType)

        if forWrite:
            if "writers" not in classConfig:
                raise RuntimeError("No writers configured for "
                        "dataset class {} used by dataset type {}".format(
                            datasetClass, datasetType))
            storages = classConfig["writers"]
        else:
            if "readers" not in classConfig:
                raise RuntimeError("No readers configured for "
                        "dataset class {} used by dataset type {}".format(
                            datasetClass, datasetType))
            storages = classConfig["readers"]

        if len(storages) != len(urlTemplates):
            raise RuntimeError("URL templates don't match storages "
                    "for dataset type {}".format(datasetType))

        neededKeys = self.getKeys(datasetType, required=True)
        neededKeys.difference_update(dataId.keys())

        if len(neededKeys) != 0 and "lookups" in datasetConfig:
            newIds = self.doLookup(neededKeys, dataId,
                    datasetConfig["lookups"])
            neededKeys.difference_update(newIds.keys())
            dataId.update(newIds)

        if len(neededKeys) != 0:
            dataIdList = ()
            if not forWrite:
                dataIdList = self.listDatasets(datasetType, dataId)
            if len(dataIdList) > 1:
                raise RuntimeError("Found multiple ({}) matches "
                        "in repository for dataset type {} "
                        "and dataId {}: dataIds {}".format(
                            len(dataIdList), datasetType, dataId, dataIdList))
            if len(dataIdList) == 0:
                raise RuntimeError("Unable to determine required "
                        "data identifiers {} for dataset type {} "
                        " and dataId {}".format(
                            neededKeys, datasetType, dataId))
            dataId = dataIdList[0]

        urls = []
        for template in urlTemplates:
            urls.append(template.format(**dataId))

        locations = []
        for i in xrange(len(urls)):
            locations.append(ButlerLocation(urls[i], storages[i], dataId))

        return locations

    def getKeys(self, datasetType=None, required=False):
        if datasetType in self.keyCache[required]:
            return self.keyCache[required][datasetType].copy()

        keys = set()
        if datasetType is None:
            for d in self.config["datasets"]:
                keys.update(self.getKeys(d, required))
        else:
            datasetConfig, datasetClass, classConfig, urlTemplates = \
                    self._parseDatasetConfig(datasetType)
            keyRegexp = re.compile(r'(?<!\{)(\{\{)*\{(\w+?)(:.*?)?\}')
            for template in urlTemplates:
                for match in keyRegexp.finditer(template):
                    keys.add(match.group(1))
            if not required:
                if "lookups" in datasetConfig:
                    for l in datasetConfig["lookups"]:
                        if "inputs" in l:
                            keys.update(l["inputs"])
                        if "outputs" in l:
                            keys.update(l["outputs"])
                if "defaults" in self.config and \
                        datasetType in self.config["defaults"]:
                    keys.update(self.config["defaults"][datasetType].keys())

        self.keyCache[required][datasetType] = keys.copy()
        return keys

    def getDatasetTypes(self):
        return self.config["datasets"].keys()

    def createDatasetType(self, datasetType, datasetClass, **kwArgs):
        pass

    def listDatasets(self, datasetType, partialDataId, **kwArgs):
        dataId = partialDataId.copy()

        datasetConfig, datasetClass, classConfig, urlTemplates = \
                self._parseDatasetConfig(datasetType)

        neededKeys = self.getKeys(datasetType, required=True)
        neededKeys.difference_update(partialDataId.keys())

        if len(neededKeys) != 0 and "lookups" in datasetConfig:
            newId = self._doLookups(neededKeys, dataId,
                    datasetConfig["lookups"])
            neededKeys.difference_update(newId.keys())
            dataId.update(newId)

        if len(neededKeys) == 0:
            dataIdList = [dataId]
        elif self.hasRegistryTable(datasetType):
            dataIdList = self._lookupByRegistry(datasetType, neededKeys,
                    dataId, datasetConfig, classConfig)
        else:
            dataIdList = self._lookupByGlob(neededKeys, urlTemplates, dataId)

        return filter(self.datasetExists, dataIdList)

###############################################################################

# set(config['inputs']).isdisjoint(config['outputs'])

    def _doLookups(self, neededKeys, dataId, lookupConfig):
        keys = neededKeys.copy()
        foundLookups = True
        lookups = []
        availableKeys = set(dataId.keys())
        while len(keys) > 0 and foundLookups:
            foundLookups = False
            for i in xrange(len(lookupConfig)):
                if index in lookups:
                    continue
                if not keys.isdisjoint(config['outputs']):
                    keys.difference_update(config['outputs'])
                    keys.update(
                            set(config['inputs']).difference(availableKeys))
                    lookups.append(i)
                    foundLookups = True
        lookups.reverse()
        newId = dataId.copy()
        for index in lookups:
            ids = self._doLookup(neededKeys.intersection(config['outputs']),
                    newId, lookupConfig)
            newId.update(ids)
        return newId

    def _parseDatasetConfig(self, datasetType):
        if datasetType not in self.config["datasets"]:
            for parent in self.parents:
                if parent.hasConfig("datasets", datasetType):
                    return parent._parseDatasetConfig(datasetType)
            raise RuntimeError("Unknown dataset type {}".format(datasetType))
        datasetConfig = self.config["datasets"][datasetType]
        datasetClass = datasetConfig["datasetClass"]
        classConfig = self.config["classes"][datasetClass]
        urlTemplates = datasetConfig["urls"]
        return datasetConfig, datasetClass, classConfig, urlTemplates

    def _lookupByGlob(self, neededKeys, urlTemplates, dataId):
        # TODO translate numbers from strings
        keyRegexp = re.compile(
                r'(?<!\{)(\{\{)*\{(' + "|".join(neededKeys) + r')(:.*?)?\}')
        dataIdList = []
        for template in urlTemplates:
            # Escape all {key}s that we are searching for.
            newTemplate = keyRegexp.sub(r'\1{{\2\3}}', template)
            # Substitute for all other {key}s from the dataId.
            newTemplate = newTemplate.format(**dataId)
            # Now replace the {key}s we are searching for with "*".
            globPattern = keyRegexp.sub("*", newTemplate)

            parseResult = urlparse.urlparse(globPattern, scheme='file')
            if parseResult.scheme != "file":
                continue

            filenames = glob.glob(os.path.join(self.repoDir, parseResult.path))

            # Make a new pattern to extract the needed keys.
            extractPattern = ""
            lastEnd = 0
            keyList = []
            for match in keyRegexp.finditer(newTemplate):
                extractPattern += re.escape(
                        match.string[lastEnd : match.start(2) - 1])
                extractPattern += r'(.+?)'
                keyList.append(match.group(2))
                lastEnd = match.end()
            extractPattern += re.escape(newTemplate[lastEnd:]) + r'$'

            # Match the new pattern against the returned filenames.
            # Note that we may have gotten extra filenames if there were
            # duplicate keys in the template.
            for filename in filenames:
                filenameOk = True
                match = extractPattern.match(filename)
                newDataId = dataId.copy()
                for value, key in zip(match.groups(), keyList):
                    if key in newDataId and newDataId[key] != value:
                        # Supposed to repeat but didn't; skip this.
                        filenameOk = False
                        break
                    newDataId[key] = value
                if filenameOk:
                    dataIdList.append(newDataId)

        return dataIdList

def _readPathConfig(repoPath):
    if os.path.isdir(repoPath):
        tempPath = os.path.join(repoPath, "_butler.sqlite3")
        if os.path.exists(tempPath):
            return _readSqliteConfig(tempPath)
        tempPath = os.path.join(repoPath, "_butler.yaml")
        if os.path.exists(tempPath):
            return _readYamlConfig(tempPath)
        tempPath = os.path.join(repoPath, "_mapper")
        if os.path.exists(tempPath):
            return _generateMapperConfig(tempPath)
        return dict(repoPath=repoPath)

    elif os.path.exists(repoPath):
        if repoPath.endswith(".sqlite3"):
            return _readSqliteConfig(repoPath)
        if repoPath.endswith(".yaml"):
            return _readYamlConfig(repoPath)
        if repoPath.endswith("_mapper"):
            return _generateMapperConfig(repoPath)
        else:
            return _generateSingleFileConfig(repoPath)
    else:
        _fatal(RuntimeError,
                "Nonexistent repository URL {}".format(repoPath))

def _readYamlConfig(path):
    with open(path) as yamlFile:
        config = yaml.load(yamlFile)
    if "repoPath" not in config:
        config["repoPath"] = os.path.dirname(path)
    return config

def _readSqliteConfig(path):
    try:
        conn = sqlite3.connect(path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT yaml FROM _config")
            result = list(cur.fetchall())
            if len(result) > 1:
                _fatal(RuntimeError, "Too many rows ({}) in "
                        "configuration database {}".format(len(result), path))
            if len(result) <= 0:
                _fatal(RuntimeError, "No data in "
                        "configuration database {}".format(path))
            config = yaml.load(result[0][0])
            if "repoPath" not in config:
                config["repoPath"] = os.path.dirname(path)
            return config
        except sqlite3.OperationalError as e:
            _fatal(RuntimeError, "sqlite error in {}: {}".format(path, e))
        finally:
            cur.close()
    finally:
        conn.close()

def _generateMapperConfig(path):
    with open(path) as f:
        mapperName = f.readline().strip()
    return dict(mapper=mapperName, repoPath=os.path.dirname(path))

def _generateSingleFileConfig(path):
    return dict(mapper="singleFileMapper.SingleFileMapper",
            singleFilePath=path,
            repoPath=os.path.dirname(path))

def _fatal(exception, message):
    log.fatal(message)
    raise exception(message)

