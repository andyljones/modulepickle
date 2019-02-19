import os
import sys
os.chdir('../drones')
sys.path.append(os.getcwd())

import ray
import modulepickle
from modulepickle.test import test
from drones.practice.contppo import gym, Agent, gae

modulepickle.extend_ray()

def f():
    env = gym.make('MountainCarContinuous-v0')
    agent = Agent()
    batch = gae.batch_rollout(agent, env, episodes=50)
    print(batch.state.shape, flush=True)
    return
    
test(f, 'cluster', ray.cloudpickle.CloudPickler)
