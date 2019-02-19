from pathlib import Path
from pkg_resources import resource_filename
import shutil
from io import BytesIO

import docker
from cloudpickle import CloudPickler

from . import extend

def test(f, image='andyljones/modulepickle', pickler=None):
    """This'll create a docker container, copy the pickling code into it, and then ask it to 
    unpickle `f` - which is presumably a function referencing modules in your working directory
    """
    client = docker.from_env()

    # Create a directory to share with the container
    path = Path('.modulepickle-test').absolute()
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)

    # Copy the pickling code in - needed so it can find the things we passed to __reduce__
    (path / __package__).mkdir(exist_ok=True, parents=True)
    shutil.copy2(resource_filename(__package__, '__init__.py'), path / __package__ / '__init__.py')

    # Dump the function
    pickler = pickler or extend(CloudPickler)
    with (path / 'f.pkl').open('wb') as pkl:
        pickler(pkl).dump(f)

    # Run the container. Or try to.
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

    # Print the results
    result = 'passed' if code == 0 else 'failed' 
    output = ''.join(f'\t{l.decode()}' for l in logs)
    print(f'Test {result}, output follows:\n\n{output}')
    