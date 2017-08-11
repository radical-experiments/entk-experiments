import rpy2
import rpy2.robjects as robjects

from rpy2.rinterface import R_VERSION_BUILD
from rpy2.robjects.packages import importr
from rpy2.robjects.packages import STAP

print('Welcome to rpy2')
print("Basic tests ...")
print(rpy2.__version__)
print(R_VERSION_BUILD)

print('Let\'s rock')
pi = robjects.r['pi']
print(pi[0])

print('Get the initial configuration as a dictionary')
with open('dummy.r', 'r') as f:
    string = f.read()
initConfig = STAP(string, 'dummy')
initConfig.dummy1()
