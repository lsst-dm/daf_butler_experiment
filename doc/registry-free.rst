Registry-free Repository
========================

A.K.A. Posix Registry.

When a repository does not have an sqlite registry the Butler will attempt to
find files in the file system based on the template in the policy. 

The registry looks for files that match the policy template at the repository 
location specified by root.
Returned paths are refined by the values in dataId.
e.g. if the template is 'raw/raw_v%(visit)d_f%(filter)s.fits.gz', 
dataId={'visit':1}, lookupProperties is ['filter'], and the
filesystem under self.root has exactly one file 'raw/raw_v1_fg.fits.gz'
then the return value will be [('g',)]

The posix registry will also look at metadata in the fits file and this may be
specified in the dataId. If an HDU number is indicated by the template then it
will try to look in that HDU first. It will fall back to the primary HDU if the
HDU number can not be determined or if the indicated HDU does not contain the
key in its metadata.