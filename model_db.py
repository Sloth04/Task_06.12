from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from config import *

Base = declarative_base()
engine = create_engine(f'sqlite:///{output}/{DATABASE_NAME}', echo=False)  # change path
Session = sessionmaker(bind=engine)


class Records(Base):
    __tablename__ = 'records'

    id = Column(Integer, primary_key=True)
    w_code = Column(String)
    id_resourse = Column(Integer, ForeignKey('resourse.id'))
    id_type_direction = Column(Integer, ForeignKey('models.id'))
    datetime = Column(DateTime)
    volume = Column(Integer)
    payment = Column(Float)

    def __init__(self, w_code: str,  id_resourse: int, id_type_direction: int,
                 datetime: DateTime, volume: int, payment: float):
        self.w_code = w_code
        self.id_resourse = id_resourse
        self.id_type_direction = id_type_direction
        self.datetime = datetime
        self.volume = volume
        self.payment = payment


class Models(Base):
    __tablename__ = 'models'

    id = Column(Integer, primary_key=True)
    service_type = Column(String)
    direction = Column(String)
    children = relationship('Records')

    def __init__(self, service_type: str, direction: str):
        self.service_type = service_type
        self.direction = direction


class Resourse(Base):
    __tablename__ = 'resourse'

    id = Column(Integer, primary_key=True)
    w_code = Column(String)
    company_alias = Column(String)
    y_code = Column(String)
    children = relationship('Records')

    def __init__(self, w_code: str, company_alias: str, y_code: str):
        self.w_code = w_code
        self.company_alias = company_alias
        self.y_code = y_code


if __name__ == '__main__':
    Base.metadata.create_all(engine)
