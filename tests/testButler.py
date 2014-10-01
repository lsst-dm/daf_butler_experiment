import butler

b = butler.Butler("tests/foo-ccd3.fits")
im = b.get("input", ccd=3)
print im
b = butler.Butler("tests/calib_repo/_butler.yaml")
b = butler.Butler("tests/raw_repo/_butler.yaml")
b = butler.Butler("tests/raw_repo/_butler.yaml",
        ["tests/calib_repo/_butler.yaml"])
b = butler.Butler("tests/output_repo", [
    "tests/raw_repo/_butler.yaml",
    "tests/calib_repo/_butler.yaml"])

import cPickle

s = cPickle.dumps(b)
print len(s)
c = cPickle.loads(s)
s = cPickle.dumps(c)
d = cPickle.loads(s)
print c.mapper.config == d.mapper.config
