from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.sql.functions import now
from base import Base
import datetime


class BookBuy(Base):
    """ Book Buy """

    __tablename__ = "book_buy"

    id = Column(Integer, primary_key=True)
    order_id = Column(String(36), nullable=False)
    book_id = Column(Integer, nullable=False)
    user_id = Column(String(36), nullable=False)
    name = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    sold = Column(Boolean, nullable=False)
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(36), nullable=False)

    def __init__(self, order_id, book_id, user_id, name, price,sold,trace_id):
        """ Initializes a book buying event """
        self.order_id = order_id
        self.book_id = book_id
        self.user_id = user_id
        self.name = name
        self.price = price
        self.sold = sold
        self.date_created = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S') # Sets the date/time record is created
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a book buy event """
        dict = {}
        dict['id'] = self.id
        dict['order_id'] = self.order_id
        dict['book_id'] = self.book_id
        dict['user_id'] = self.user_id
        dict['name'] = self.name
        dict['price'] = self.price
        dict['sold'] = self.sold
        dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id

        return dict
