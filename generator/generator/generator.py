import numpy as np
from datetime import datetime
import datetime as dt
import json

FORMAT = "%Y-%m-%d %H:%M:%S"

class DcsGenerator():
    def __init__(self):
        np.random.seed(1234)

    def generate(self):
        date = datetime.now().strftime(FORMAT)
        obj =  dict(
            date=date,
            TagA=np.random.randn(),
            TagB=np.random.randn(),
            TagC=np.random.randn(),
            TagD=np.random.randn()
        )
        return  json.dumps(obj)

class QualityGenerator():
    def __init__(self):
        np.random.seed(1234)
        self._counter = 0

    def generate(self):
        start_time = datetime.now().strftime(FORMAT)
        end_time = (datetime.now() + dt.timedelta(hours=1)).strftime(FORMAT)
        obj = dict(
            lotID=self._counter,
            start_time=start_time,
            end_time=end_time,
            quality=np.random.randint(5)
        )
        self._counter += 1
        return  json.dumps(obj)

class SpectralGenerator():
    def __init__(self):
        np.random.seed(1234)

    def generate(self):
        date = datetime.now().strftime(FORMAT)
        obj = dict(
            date=date,
            spectral=np.random.rand(3000).tolist()
        )
        return  json.dumps(obj)
