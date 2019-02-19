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

__all__ = ('extend', 'extend_ray', 'extend_cloudpickle')

log = getLogger(__name__)

TEMPDIR_ID = 'MODULEPICKLE'

def md5(compressed):
    md5 = hashlib.md5()
    md5.update(compressed)
    return md5.hexdigest()[:16]  # 32 bytes ought to be enough for everyone

class Package():

    def __init__(self, name, compressed):
        self.name = name
        self.compressed = compressed
        self.md5 = md5(compressed)

    def invalidate_caches(self):
        # Chuck out any modules that come from one of our temp dirs, so that when they get importer next time it's imported from
        # the shiny new temp dir
        for k, v in sys.modules.items():
            filepath = getattr(v, '__file__', '') or ''
            if f'{TEMPDIR_ID}-{self.name}-' in filepath:
                del sys.modules[k]

        # And then invalidate the cache of everyone on the meta_path, just to be safe.
        importlib.invalidate_caches()

    def uninstall(self):
        sys.path = [p for p in sys.path if f'{TEMPDIR_ID}-{self.name}-' not in p]

    def extract(self):
        # Salt the temp directory with the hashcode of the compressed dir, so that when the next copy of it comes down the line,
        #  we can either reuse the existing dir if it's the same, or point ourselves at a new one if it isn't.
        dirpath = tempfile.mkdtemp(prefix=f'{TEMPDIR_ID}-{self.name}-{self.md5}-')
        bs = BytesIO(self.compressed)
        with TarFile(fileobj=bs) as tf:
            tf.extractall(os.path.join(dirpath))
        return dirpath

    def install(self):
        """'Installing' this package means extracting it to a hash-salted temp dir and then appending the dir to the path"""
        # Only need to install it if the hash of the dir has changed since we last added it to the path
        if not any(self.md5 in p for p in sys.path):
            self.uninstall()
            self.invalidate_caches()
            sys.path.append(self.extract())

    def load(self, name):
        self.install()
        return importlib.import_module(name)

def compress(packagename):
    tar = BytesIO()
    with TarFile(fileobj=tar, mode='w') as tf:
        tf.add(packagename, packagename)
    #TODO: This was originally gzipped, but the gzipped value seems to change on repeated compressions, breaking hashing.
    # Looks like the issue is a timestamp that can be overriden with a parameter, but let's leave it uncompressed for now.
    return tar.getvalue()

def import_compressed(name, package):
    return package.load(name)

def import_global(module, obj):
    return obj

def packagename(module):
    # The package we want to zip up is the first part of the module name
    #TODO: Check this holds on relative imports
    return module.__name__.split('.')[0]

def is_local(module):
    # If the module is in the current working directory, 
    # and it doesn't have `site-packages` in it's path (which probably means it's part of a local virtualenv)
    # assume it's local and that it's cool to pickle it.
    path = getattr(module, '__file__', '')
    return path.startswith(os.getcwd()) and ('site-packages' not in path)

def extend(base):
    """Create a Pickler that can pickle packages by inheriting from `base`
    
    We're dynamically inheriting from `base` here because my principal use case is extending ray's pickler, and ray's 
    installation dependencies are vast. Don't want to truck that around for a one-module package which works just as 
    well with cloudpickle.
    """

    class ModulePickler(base):

        dispatch = base.dispatch.copy()

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.packages = {}

        def compress_package(self, name):
            # The same package might contain many of the modules a function references, so it makes sense to cache them
            # as we go.
            if name not in self.packages:
                compressed = compress(name)
                self.packages[name] = Package(name, compressed)
            return self.packages[name]
        
        def save_module(self, obj):
            if is_local(obj):
                args = (obj.__name__, self.compress_package(packagename(obj)))
                return self.save_reduce(import_compressed, args, obj=obj)
            else:
                return super().save_module(obj)

        dispatch[types.ModuleType] = save_module

        def save_global(self, obj, *args, **kwargs):
            module = sys.modules[obj.__module__]
            # This is a dumb trick to handle my incomprehension of pickletools.
            # The problem is that sometimes a global will be unpickled before it's module is, which will throw an error.
            # Here, if we haven't seen the package before, we require it to reconstruct the global.
            # There is surely a better way if you understand the pickle VM better than I do.
            if is_local(module) and (packagename(module) not in self.packages):
                args = (module, obj)
                return self.save_reduce(import_global, args, obj=obj)
            return super().save_global(obj, *args, **kwargs)
        dispatch[type] = save_global
        dispatch[types.ClassType] = save_global
    
    return ModulePickler

def extend_ray():
    """Extends Ray's CloudPickler with a ModulePickler"""
    import ray
    import ray.cloudpickle
    ray.cloudpickle.CloudPickler = extend(ray.cloudpickle.CloudPickler)
    ray.cloudpickle.dump.__globals__['CloudPickler'] = ray.cloudpickle.CloudPickler
    ray.cloudpickle.dumps.__globals__['CloudPickler'] = ray.cloudpickle.CloudPickler

def extend_cloudpickle():
    """Extends cloudpickle's CloudPickler with a ModulePickler"""
    import cloudpickle
    cloudpickle.CloudPickler = extend(cloudpickle.CloudPickler)
    cloudpickle.dump.__globals__['CloudPickler'] = cloudpickle.CloudPickler
    cloudpickle.dumps.__globals__['CloudPickler'] = cloudpickle.CloudPickler
