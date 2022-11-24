# This is a sample Python script.
import os
import sys
HOME_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(HOME_DIR)
print(sys.path)
import utils.dataUtils
