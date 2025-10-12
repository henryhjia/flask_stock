from dotenv import load_dotenv
load_dotenv()
import os
from flask import Flask, render_template, request, jsonify
import yfinance as yf
import pandas as pd
import numpy as np
from google.cloud.sql.connector import Connector


app = Flask(__name__)

def get_db_connection():
    connector = Connector()
    conn = connector.connect(
        os.environ["INSTANCE_CONNECTION_NAME"],  # e.g. "project:region:instance"
        "pg8000",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"].strip(),
        db=os.environ["DB_NAME"],
    )
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    ticker = request.form['ticker']
    start_date = request.form['start_date']
    end_date = request.form['end_date']

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT * FROM stock WHERE ticker = %s AND start_date = %s AND end_date = %s', (ticker, start_date, end_date))
        row = cur.fetchone()
        existing_stock = None
        if row:
            columns = [col[0] for col in cur.description]
            existing_stock = dict(zip(columns, row))

        if existing_stock:
            return jsonify({
                'ticker': existing_stock['ticker'],
                'start_date': existing_stock['start_date'],
                'end_date': existing_stock['end_date'],
                'max_price': existing_stock['max_price'],
                'min_price': existing_stock['min_price'],
                'mean_price': existing_stock['mean_price'],
                'message': 'Data already exists in the database.'
            })

        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
        if data.empty:
            return jsonify({'error': 'No data found for the given ticker and date range.'}), 400

        max_price = float(np.max(data['Close']))
        min_price = float(np.min(data['Close']))
        mean_price = float(np.mean(data['Close']))

        cur.execute('''
            INSERT INTO stock (ticker, start_date, end_date, max_price, min_price, mean_price)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (ticker, start_date, end_date, max_price, min_price, mean_price))
        conn.commit()

        return jsonify({
            'ticker': ticker,
            'start_date': start_date,
            'end_date': end_date,
            'max_price': max_price,
            'min_price': min_price,
            'mean_price': mean_price
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/history')
def history():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM stock ORDER BY id DESC')
    columns = [col[0] for col in cur.description]
    stocks = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return render_template('history.html', stocks=stocks)

if __name__ == '__main__':
    # The init_db() function should be called manually or with a separate script
    # when using a persistent database like PostgreSQL.
    app.run(debug=True)