from flask import Flask, jsonify, request
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import threading

app = Flask(__name__)
Base = declarative_base()


# Определите ваши модели базы данных здесь
class Restaurant(Base):
    __tablename__ = 'restaurants'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Review(Base):
    __tablename__ = 'reviews'
    id = Column(Integer, primary_key=True)
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))
    rating = Column(Integer)
    comment = Column(String)
    restaurant = relationship("Restaurant", backref="reviews")


engine = create_engine('postgresql://myuser:mypassword@database/mydatabase')
Session = sessionmaker(bind=engine)
session = Session()

consumer = KafkaConsumer('test-topic', bootstrap_servers='kafka:9092')


@app.route('/search', methods=['GET'])
def search():
    review_id = request.args.get('id')

    if review_id:
        result = session.query(Review).filter_by(id=review_id).first()

        if result:
            return jsonify(result.__dict__)
        else:
            return jsonify({'message': 'Отзыв не найден'})
    else:
        results = session.query(Review).all()
        return jsonify([r.__dict__ for r in results])

@app.route('/reports', methods=['GET'])
def get_reports():
    most_visited_restaurants = session.query(Restaurant.name, func.count(Review.id)).join(Review).group_by(
        Restaurant.name).order_by(func.count(Review.id).desc()).limit(10).all()

    top_rated_restaurants = session.query(Restaurant.name, func.avg(Review.rating)).join(Review).group_by(
        Restaurant.name).order_by(func.avg(Review.rating).desc()).limit(10).all()

    reports = {
        'most_visited_restaurants': [{'restaurant_name': name, 'visit_count': count} for name, count in most_visited_restaurants],
        'top_rated_restaurants': [{'restaurant_name': name, 'average_rating': avg_rating} for name, avg_rating in top_rated_restaurants]
    }
    return jsonify(reports)

@app.route('/process_data', methods=['POST'])
def process_data():
    data = request.get_json()
    process_received_data(data)
    return 'Data processed'


def process_received_data(data):
    # Обработайте полученные данные и сохраните их в базе данных
    for item in data:
        restaurant = Restaurant(name=item['restaurant_name'])
        session.add(restaurant)
        session.commit()

        review = Review(restaurant_id=restaurant.id, rating=item['rating'], comment=item['comment'])
        session.add(review)
        session.commit()


def kafka_consumer():
    consumer = KafkaConsumer('test-topic', bootstrap_servers='kafka:9092')

    # Получение сообщений из Kafka
    for message in consumer:
        process_message(message.value)

if __name__ == '__main__':
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.start()
    app.run(debug=True, host='0.0.0.0',port=5000)