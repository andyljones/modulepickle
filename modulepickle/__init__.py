import os
import types
from io import BytesIO
from tarfile import TarFile
import tempfile
import importlib.machinery
import hashlib
import sys
from logging import getLogger

log = getLogger(__name__)

__all__ = ('pickler',)

def md5(compressed):
    md5 = hashlib.md5()
    md5.update(compressed)
    return md5.digest()

class Package(object):

    def __init__(self, name, compressed):
        self.name, self.compressed = name, compressed
        self.hash = md5(compressed)
        self.extracted = False
        self.finder = None
        self.path = None

    def extract(self):
        if self.path is None:
            dirpath = tempfile.mkdtemp()
            bs = BytesIO(self.compressed)
            with TarFile(fileobj=bs) as tf:
                tf.extractall(os.path.join(dirpath, self.name))

            self.finder = importlib.machinery.PathFinder()
            self.path = dirpath
        return self.path

    def load(self, modulename):
        # If the module is missing, or if it's hash is old
        if (modulename not in sys.modules) or (sys.modules[modulename].__packagehash__ != self.hash):
            log.debug(f'Loading code of {modulename}')
            path = self.extract()

            # Following `https://docs.python.org/3/reference/import.html#loading`
            spec = self.finder.find_spec(modulename, [path])
            module = types.ModuleType(spec.name)
            importlib._bootstrap._init_module_attrs(spec, module)
            module.__packagehash__ = self.hash
            sys.modules[modulename] = module
            spec.loader.exec_module(module)

        return sys.modules[modulename]

def compress(packagename):
    tar = BytesIO()
    with TarFile(fileobj=tar, mode='w') as tf:
        tf.add(packagename, '')
    #TODO: This was originally gzipped, but the gzipped value seems to change on repeated compressions, breaking hashing.
    # Looks like the issue is a timestamp that can be overriden with a parameter, but let's leave it uncompressed for now.
    return Package(packagename, tar.getvalue())

def import_compressed(modulename, package):
    return package.load(modulename)

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

        def compress_package(self, package):
            if package not in self.packages:
                self.packages[package] = compress(package)
            return self.packages[package]

        def save_module(self, obj):

            # If the module isn't in the current working directory, use the default implementation
            path = getattr(obj, '__file__', '')
            if not path.startswith(os.getcwd()):
                log.debug(f'Saving reference only of {obj.__name__}')
                return super().save_module(obj)
            log.debug(f'Saving code of {obj.__name__}')

            # Otherwise the package we want to zip up is the first part of the module name
            #TODO: Check this holds on relative imports
            package = obj.__name__.split('.')[0]


            args = (obj.__name__, self.compress_package(package))
            self.save_reduce(import_compressed, args, obj=obj)
        dispatch[types.ModuleType] = save_module
    
    return ModulePickler
