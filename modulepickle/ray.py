import ray
import ray.cloudpickle
from . import extend

def install():
    """Extends Ray's CloudPickler with a ModulePickler"""
    ray.cloudpickle.CloudPickler = extend(ray.cloudpickle.CloudPickler)
