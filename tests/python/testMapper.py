import yaml

from mapper import Mapper

class TestMapper(Mapper):
    def __init__(self, config, source):
        classConfig = yaml.load("""
classes:
  exposure:
    readers: [exposureFits.ExposureFits.get]
    writers: [exposureFits.ExposureFits.put]
  image:
    readers: [imageFits.ImageFits.get]
    writers: [imageFits.ImageFits.put]
  propertyset:
    readers: [propertySetYaml.PropertySetYaml.get]
    writers: [propertySetYaml.PropertySetYaml.put]
  cameraGeom:
    readers: [cameraGeom.CameraGeom.get]
    writers: [cameraGeom.CameraGeom.put]
  config:
    readers: [config.Config.get]
    writers: [config.Config.put]
  ampExposureId:
    readers: [id.ExposureId.getAmpId]
  ccdVisitId:
    readers: [id.ExposureId.getCcdVisit]
""")
        config['classes'] = classConfig['classes']
        super(TestMapper, self).__init__(config, source)
