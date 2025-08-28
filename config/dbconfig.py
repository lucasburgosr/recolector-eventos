from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

engine = create_engine('mysql+pymysql://root:root@localhost/eventos', pool_pre_ping=True)
# engine = create_engine('mysql+pymysql://observer:8Vko*OH0Xv@149.50.141.218:3306/eventos', pool_pre_ping=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()