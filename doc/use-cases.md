# Sample Use Cases

## Reading One File

```
import lsst.daf.butler as dafButler

butler = dafButler.Butler(outputRepo="outputDir", inputRepos=["myRawData.fits"])
butler.defineAlias("image", "raw")
image = butler.get("@image", filename="myRawData.fits")
image += 1
butler.createDatasetType("processed", "imageGenre")
butler.put(image, "processed", filename="myProcessedData.fits")
```

## Directory in a Filesystem

```
import lsst.daf.butler as dafButler

butler = dafButler.Butler(outputRepo="outputDir", inputRepository=["inputDir"])
for dataRef in butler.getRefSet("raw", visit=1234):
    ccdExposure = dataRef.get("raw")
    ccdMetadata = dataRef.get("raw_md")
    ccdExposureIdentifier = dataRef.get("ccdExposureId") # long
```

## Database Access

```
import lsst.daf.butler as dafButler

butler = dafButler.Butler(outputRepo="outputDir", inputRepo="sqlite:///myDb.sqlite3")
table = butler.get("src", where="mag_r < 22 and mag_i - mag_r > 0.1")
```

## Rendezvous

```
import lsst.daf.butler as dafButler

butler = dafButler.Butler(outputRepo="outputDir", inputRepos=["inputDir", "calibDir"])
for dataRef in butler.getRefSet("raw", visit=1234):
    ccdExposure = dataRef.get("raw")
    dark = dataRef.get("dark") # looks up based on raw exposure midpoint and dark validity interval
    flat = dataRef.get("flat") # looks up based on time and filter
```
