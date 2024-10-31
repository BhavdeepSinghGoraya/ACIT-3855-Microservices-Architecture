from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.sql.functions import now
from base import Base
import datetime


class BookSell(Base):
    """ Book Sell """

    __tablename__ = "book_sell"

    id = Column(Integer, primary_key=True)
    book_id = Column(Integer, nullable=False)
    user_id = Column(String(36), nullable=False)
    name = Column(String(100), nullable=False)
    listing_date = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    genre = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(36), nullable=False)

    def __init__(self, book_id, user_id, name, listing_date, price, genre,trace_id):
        """ Initializes a book selling event """
        self.book_id = book_id
        self.user_id = user_id
        self.name = name
        self.listing_date = listing_date
        self.price = price
        self.genre = genre
        self.date_created = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S') # Sets the date/time record is created
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a book sell event """
        dict = {}
        dict['id'] = self.id
        dict['book_id'] = self.book_id
        dict['user_id'] = self.user_id
        dict['name'] = self.name
        dict['listing_date'] = self.listing_date
        dict['price'] = self.price
        dict['genre'] = self.genre
        dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id

        return dict
