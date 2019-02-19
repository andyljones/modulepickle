import os
import types
from io import BytesIO
from tarfile import TarFile
import tempfile
import importlib
import importlib.machinery
import hashlib
import sys
from logging import getLogger

__all__ = ('extend',)

log = getLogger(__name__)

TEMPDIR_ID = 'MODULEPICKLE'

def md5(compressed):
    md5 = hashlib.md5()
    md5.update(compressed)
    return md5.hexdigest()[:16]

def extract(hashcode, compressed):
    dirpath = tempfile.mkdtemp(prefix=f'{TEMPDIR_ID}-{hashcode}-')
    bs = BytesIO(compressed)
    with TarFile(fileobj=bs) as tf:
        tf.extractall(os.path.join(dirpath))
    return dirpath

def compress(packagename):
    tar = BytesIO()
    with TarFile(fileobj=tar, mode='w') as tf:
        tf.add(packagename, packagename)
    #TODO: This was originally gzipped, but the gzipped value seems to change on repeated compressions, breaking hashing.
    # Looks like the issue is a timestamp that can be overriden with a parameter, but let's leave it uncompressed for now.
    return tar.getvalue()

def invalidate_caches():
    for k, v in sys.modules.items():
        filepath = getattr(v, '__file__', '') or ''
        if TEMPDIR_ID in filepath:
            del sys.modules[k]
    importlib.invalidate_caches()

def uninstall():
    sys.path = [p for p in sys.path if TEMPDIR_ID not in p]

class Package():

    def __init__(self, compressed):
        self.compressed = compressed
        self.md5 = md5(compressed)

    def install(self):
        if any(self.md5 in p for p in sys.path):
            return 

        uninstall()
        invalidate_caches()
        sys.path.append(extract(self.md5, self.compressed))

    def load(self, name):
        self.install()
        return importlib.import_module(name)

def import_compressed(name, package):
    return package.load(name)

def extend(base):
    """Create a Pickler that can pickle packages by inheriting from `base`
    
    We're dynamically inheriting from `base` here because my principal use case is extending ray's pickler, and ray's 
    installation dependencies are vast. Don't want to truck that around for a one-module package.
    """

    class ModulePickler(base):

        dispatch = base.dispatch

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.packages = {}

        def compress_package(self, packagename):
            if packagename not in self.packages:
                compressed = compress(packagename)
                self.packages[packagename] = Package(compressed)
            return self.packages[packagename]

        def save_module(self, obj):
            # If the module isn't in the current working directory, 
            # or module has site-packages in it's path (to exclude local envs)
            # use the default implementation
            path = getattr(obj, '__file__', '')
            if (not path.startswith(os.getcwd())) or ('site-packages' in path):
                log.debug(f'Saving reference only of {obj.__name__}')
                return super().save_module(obj)
            log.debug(f'Saving code of {obj.__name__}')

            # Otherwise the package we want to zip up is the first part of the module name
            #TODO: Check this holds on relative imports
            packagename = obj.__name__.split('.')[0]

            args = (obj.__name__, self.compress_package(packagename))
            self.save_reduce(import_compressed, args, obj=obj)
        dispatch[types.ModuleType] = save_module
    
    return ModulePickler
