from butlerLocation import ButlerLocation
from mapper import Mapper

class SingleFileMapper(Mapper):
    def __init__(self, config, source):
        config['datasets'] = {
                'input': {
                    'datasetClass': 'exposure',
                    'urls': [config['singleFilePath']]
                    }
                }
        config['classes'] = {
                'exposure': {
                    'readers': ['exposureFits.ExposureFits.get'],
                    'writers': ['exposureFits.ExposureFits.put']
                    }
                }
        super(SingleFileMapper, self).__init__(config, source)
