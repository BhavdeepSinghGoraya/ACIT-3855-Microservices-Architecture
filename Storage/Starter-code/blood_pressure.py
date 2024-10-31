from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql.functions import now
from base import Base
import datetime


class BloodPressure(Base):
    """ Blood Pressure """

    __tablename__ = "blood_pressure"

    id = Column(Integer, primary_key=True)
    patient_id = Column(String(250), nullable=False)
    device_id = Column(String(250), nullable=False)
    timestamp = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    systolic = Column(Integer, nullable=False)
    diastolic = Column(Integer, nullable=False)

    def __init__(self, patient_id, device_id, timestamp, systolic, diastolic):
        """ Initializes a blood pressure reading """
        self.patient_id = patient_id
        self.device_id = device_id
        self.timestamp = timestamp
        self.date_created = now() # Sets the date/time record is created
        self.systolic = systolic
        self.diastolic = diastolic

    def to_dict(self):
        """ Dictionary Representation of a blood pressure reading """
        dict = {}
        dict['id'] = self.id
        dict['patient_id'] = self.patient_id
        dict['device_id'] = self.device_id
        dict['blood_pressure'] = {}
        dict['blood_pressure']['systolic'] = self.systolic
        dict['blood_pressure']['diastolic'] = self.diastolic
        dict['timestamp'] = self.timestamp
        dict['date_created'] = self.date_created

        return dict
