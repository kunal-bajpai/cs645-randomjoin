import time

from containers import Relation, Tuple
from operations import *
from queries import *
from algos import *


data = read_data()

Q3(data, ExtendedOlken)
Q3(data, ExactWeightChain)
QX(data, ExactWeightChain)
Q3(data, OnlineExploration)
QX(data, OnlineExploration)
