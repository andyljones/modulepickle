"""
## TODO
  * Import parents
  * Change to a parallel sys.modules
"""
import os
import types
from io import BytesIO
from tarfile import TarFile
import tempfile
import importlib
import importlib.machinery
import hashlib
import sys
import re
from logging import getLogger
from collections import namedtuple

log = getLogger(__name__)

__all__ = ('pickler',)

LOADERS = {}

def md5(compressed):
    md5 = hashlib.md5()
    md5.update(compressed)
    return md5.digest()

def extract(compressed):
    dirpath = tempfile.mkdtemp()
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

class Loader():

    def __init__(self, compressed):
        self.root = extract(compressed)

        self.original_modules = sys.modules.copy()
        self.modules = self.original_modules.copy()

        self.original_path = sys.path.copy()
        self.path = self.original_path + [self.root]

    def load(self, name):
        sys.path = self.path
        module = importlib.import_module(name)
        sys.path = self.original_path
        return module

Package = namedtuple('Package', ('hash', 'compressed'))

def import_compressed(modulename, package):
    if package.hash not in LOADERS:
        LOADERS[package.hash] = Loader(package.compressed)
    return LOADERS[package.hash].load(modulename)

def pickler(base):
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
                self.packages[packagename] = Package(md5(compressed), compressed)
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
