import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
import json
from models import create_tables, Publisher, Shop, Book, Stock, Sale




def add_data_to_db(session):
    with open('tests_data.json', 'r') as fd:
        data = json.load(fd)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()


def get_info(publisher_info, session):
    q = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).\
        select_from(Shop).\
        join(Shop.stocks_shops).\
        join(Stock.books_stocks).\
        join(Book.publishers_books).\
        join(Stock.sales_stocks)
    if(publisher_info.isdigit()):
        q_filtered = q.filter(Publisher.id == publisher_info).all()
    else:
        q_filtered = q.filter(Publisher.name == publisher_info).all()
    for book_title, shop_name, price, sale_date in q_filtered:
        print(f"{book_title} | {shop_name} | {price} | {sale_date.strftime('%d-%m-%Y')}")


if __name__ == '__main__':
    user = input('user: ')
    password = input('password: ')
    hostname = input('hostname: ')
    port = input('port: ')
    db_name = input('db_name: ')

    DSN = f"postgresql://{user}:{password}@{hostname}:{port}/{db_name}"
    engine = sq.create_engine(DSN)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    add_data_to_db(session)

    publisher_info = input('Введите имя или id автора: ')
    get_info(publisher_info, session)