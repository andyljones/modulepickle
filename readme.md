**This is not yet stable**

modulepickle extends [cloudpickle](https://github.com/cloudpipe/cloudpickle) and adds support for pickling whole packages from your working directory. This is useful when you're developing those modules and don't want to manually ship them out every time you make a remote function call.

**WARNING: If your code defers it's imports - if there are import statements hidden inside functions - you risk [nasal demons](http://www.catb.org/jargon/html/N/nasal-demons.html).**

### Installation 
Install with 

```
pip install modulepickle
```

then anywhere in your code, call one of

```python
import modulepickle; modulepickle.extend_ray()  # to extend the Ray pickler
import modulepickle; modulepickle.extend_cloudpickle()  # to extend the CloudPickle pickler
```

### Demo
The easiest way to test things out is with [Docker](https://www.docker.com/). Once you have it installed, install the libraries that the test file uses with

```
pip install cloudpickle docker
```

Then if you run something like

```python
import other  # or any package in your working directory 
import modulepickle.test

def f():
    assert other.__name__ == 'other'
    
modulepickle.test.test(f)
```

the function `f` will be pickled along with the directory containing the module `other`, and the whole thing will be shipped to - and executed in - the docker container.

### Internals
modulepickle works by changing how modules are serialized. In both regular pickle and cloudpickle, modules are serialized with just a name, and it's the unpickler's job to figure out what that name refers to. For modules that are fixed over long periods - like your numpy install - that's just fine, and any old dependency manager will ensure that the module that's used on one end is the same used on the other.

For modules that change frequently though - like the ones in your working directory - rebuilding and redeploying a bunch of containers every time you fix a bug is a pain in the ass. modulepickle's way around this is to send not just a reference to a module, but _the entire package from your working directory_. This would be wasteful in terms of bandwidth and local compute, except that in domains like machine learning  evaluating the function itself is usually far, far more expensive that anything you care to do with your source tree.

So, that's what goes into the pickle. What happens on the other end? Well, little do most callers of `pickle.dump` know, but [there's a whole VM down there](https://docs.python.org/3/library/pickletools.html). When the module is unpickled, it gets unzipped into a temporary directory whose name holds a hash of the directory's contents. This directory is added to the path, and from thereon out good ol' `import` works as you expect.

The more interesting part is when the _next_ function gets unpickled. If the working directory it references is the same as before, then the old temp dir is reused. If it's different though, then all the modules referencing the old dir get purged from `sys.modules`, the old temp dir gets removed from the path, and a new temp dir containing a copy of the new working directory is added. As long as there are no deferred imports of modules hanging around in some dusty subfunction (and there shouldn't be!), this should all be cheery - the old function will hold onto it's references to the old modules, and the new function will get to import the new modules. If your code _does_ defer it's imports though, it might end up importing after the temp dirs have been switched out, meaning your old function could import new code, meaning nasal demons.

### Notes
This is the end result of several more complex approaches to the same problem. Originally I wanted to make full use of the extensibility in [Python's import system](https://docs.python.org/3/reference/import.html) and write something that, when unpickled, would insert itself into `sys.meta_path` and handle the unpickling from there. This will still probably work, but it turns out writing bug-free loaders and importers is a massive pain, and it's much easier to build off of the import system that already exists. 