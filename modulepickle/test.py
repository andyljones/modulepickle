from pathlib import Path
from pkg_resources import resource_filename
import shutil
from io import BytesIO

import docker
from cloudpickle import CloudPickler

from . import extend

def test(f, image='modulepickle', pickler=None):
    """Run `docker build -t modulepickle .` to create the image this needs
    
    This'll create a docker container, copy the pickling code into it, and then ask it to 
    unpickle `f` - which is presumably a function referencing modules in your working directory

    ```
    import other
    import modulepickle.test

    def f():
        assert other.__name__ == 'other'
        
    modulepickle.test.test(f)
    ```

    If it fails, turn the logging level down to DEBUG and check that `other` is in your working directory

    """
    client = docker.from_env()

    path = Path('output/pickle').absolute()
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)

    # Copy the pickling code in - needed so it can find the things we passed to __reduce__
    (path / __package__).mkdir(exist_ok=True, parents=True)
    shutil.copy2(resource_filename(__package__, '__init__.py'), path / __package__ / '__init__.py')

    pickler = pickler or extend(CloudPickler)
    with (path / 'f.pkl').open('wb') as pkl:
        pickler(pkl).dump(f)

    command = """python -c "import pickle; pickle.load(open('/host/f.pkl', 'rb'))()" """
    volume = {str(path): {'bind': '/host', 'mode': 'rw'}}
    try:
        container = client.containers.create(image, command, volumes=volume, working_dir='/host')
        container.start()
        logs = container.logs(stdout=True, stderr=True, stream=True, follow=True)
        code = container.wait()['StatusCode']
    finally:
        container.stop()
        container.remove()

    result = 'passed' if code == 0 else 'failed' 
    output = ''.join(f'\t{l.decode()}' for l in logs)
    print(f'Test {result}, output follows:\n\n{output}')
    