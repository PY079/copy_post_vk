from sqlalchemy import create_engine, Column, Integer, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import mmh3


def add_text(text=str):
    # Создание базы данных SQLite
    _engine = create_engine('sqlite:///C:/Users/User/Desktop/tg_bot_mus/vk_p/pv/post_text.db', echo=False)

    # Создание базового класса

    _Base = declarative_base()

    # Определение класса таблицы
    class MyTable(_Base):
        __tablename__ = 'my_table'
        number = Column(Integer, primary_key=True)
        post_text = Column(Text)
       

    # _Base.metadata.create_all(_engine)
    
    # Создание сессии для работы с базой данных
    _Session = sessionmaker(bind=_engine)
    _session = _Session()
    hashed_text = mmh3.hash(text)
    result = _session.query(MyTable).filter(MyTable.post_text==hashed_text).first()
    if result is None:
        new_record = MyTable(post_text=hashed_text)
        _session.add(new_record)
        _session.commit()
        _session.close()
        return True
    else:
        _session.close()
        return False

def upd_id(id):
    _engine = create_engine('sqlite:///C:/Users/User/Desktop/tg_bot_mus/vk_p/pv/gho.db', echo=False)
    _Base = declarative_base()

    class Name(_Base):
        __tablename__ = 'name'
        new_id = Column(Integer, primary_key=True)

    _Session = sessionmaker(bind=_engine)
    _session = _Session()


    _session.query(Name).update({'new_id': id})
    _session.commit()
    _session.close()
           
        



def pos_id(wal):

    _engine = create_engine('sqlite:///C:/Users/User/Desktop/tg_bot_mus/vk_p/pv/gho.db', echo=False)
    _Base = declarative_base()

    class Name(_Base):
        __tablename__ = 'name'
        new_id = Column(Integer, primary_key=True)

    _Session = sessionmaker(bind=_engine)
    _session = _Session()

    id_latest_post = wal['count']
    # print(id_latest_post)

    id_next_post = id_latest_post + 1


    ew = _session.query(Name.new_id).first()
    count_post_bd = ew[0]

    if count_post_bd <= id_latest_post:
        asd = id_latest_post - ew[0]

        if asd > 0:
            new_id = ew[0] + 1
            _session.query(Name).update({'new_id': new_id})
            _session.commit()
            return asd
        else:
            new_id = ew[0] + 1
            _session.query(Name).update({'new_id': new_id})
            _session.commit()
        

    if count_post_bd > id_next_post:
        new_id = count_post_bd - id_next_post
        _session.query(Name).update({'new_id': count_post_bd - new_id})
        _session.commit()
        print(f'В БД -- {count_post_bd} изменилось на -- {count_post_bd - new_id}')
    _session.close()