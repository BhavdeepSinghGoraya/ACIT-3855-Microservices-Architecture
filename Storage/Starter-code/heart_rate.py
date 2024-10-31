from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql.functions import now
from base import Base
import datetime


class HeartRate(Base):
    """ Heart Rate """

    __tablename__ = "heart_rate"

    id = Column(Integer, primary_key=True)
    patient_id = Column(String(250), nullable=False)
    device_id = Column(String(250), nullable=False)
    timestamp = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    heart_rate = Column(Integer, nullable=False)

    def __init__(self, patient_id, device_id, timestamp, heart_rate):
        """ Initializes a heart rate reading """
        self.patient_id = patient_id
        self.device_id = device_id
        self.timestamp = timestamp
        self.date_created = now() # Sets the date/time record is created
        self.heart_rate = heart_rate

    def to_dict(self):
        """ Dictionary Representation of a heart rate reading """
        dict = {}
        dict['id'] = self.id
        dict['patient_id'] = self.patient_id
        dict['device_id'] = self.device_id
        dict['heart_rate'] = self.heart_rate
        dict['timestamp'] = self.timestamp
        dict['date_created'] = self.date_created

        return dict
