"""
use postgreSQL for testing
"""
import pytest
import pandas as pd
from app import app
import testing.postgresql
import psycopg2
import os

# SQL to create a schema for the test database
CREATE_TABLE_SQL = """
CREATE TABLE stock (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    max_price REAL NOT NULL,
    min_price REAL NOT NULL,
    mean_price REAL NOT NULL,
    UNIQUE (ticker, start_date, end_date)
);
"""

@pytest.fixture
def client(mocker):
    """
    Test fixture that sets up a test client for the Flask app.
    It creates a temporary PostgreSQL database and sets the necessary
    environment variables for the app to connect to it.
    """
    with testing.postgresql.Postgresql() as postgresql:
        # Create the stock table in the temporary database
        conn = psycopg2.connect(**postgresql.dsn())
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        cur.close()
        conn.close()

        # Set environment variables for the app to connect to the test database
        os.environ["INSTANCE_CONNECTION_NAME"] = postgresql.dsn()['host']
        os.environ["DB_USER"] = postgresql.dsn()['user']
        os.environ["DB_PASS"] = postgresql.dsn()['password']
        os.environ["DB_NAME"] = postgresql.dsn()['dbname']
        
        # Mock the get_db_connection function to connect to the test database
        def mock_get_db_connection():
            return psycopg2.connect(**postgresql.dsn())

        mocker.patch('app.get_db_connection', side_effect=mock_get_db_connection)

        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

# Test for the index page
def test_index(client):
    """Test the index page."""
    rv = client.get('/')
    assert rv.status_code == 200
    assert b'Stock Data Analyzer' in rv.data

# Test for the history page
def test_history(client):
    """Test the history page."""
    rv = client.get('/history')
    assert rv.status_code == 200
    assert b'Stock Data History' in rv.data

# Test processing a new stock
def test_process_new(client, mocker):
    """Test processing a new stock ticker."""
    # Mock yfinance.download
    mock_data = pd.DataFrame({'Close': [100, 150, 125]})
    mocker.patch('yfinance.download', return_value=mock_data)

    response = client.post('/process', data={
        'ticker': 'AAPL',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['ticker'] == 'AAPL'
    assert json_data['max_price'] == 150.0
    assert json_data['min_price'] == 100.0
    assert json_data['mean_price'] == 125.0

# Test processing a duplicate stock
def test_process_duplicate(client, mocker):
    """Test processing a duplicate stock ticker."""
    # Mock yfinance.download
    mock_data = pd.DataFrame({'Close': [100, 150, 125]})
    mocker.patch('yfinance.download', return_value=mock_data)

    # First request to insert the data
    client.post('/process', data={
        'ticker': 'GOOG',
        'start_date': '2023-02-01',
        'end_date': '2023-02-28'
    })

    # Second request with the same data
    response = client.post('/process', data={
        'ticker': 'GOOG',
        'start_date': '2023-02-01',
        'end_date': '2023-02-28'
    })

    assert response.status_code == 200
    json_data = response.get_json()
    assert 'Data already exists' in json_data['message']

# Test for case where no data is found
def test_process_no_data(client, mocker):
    """Test processing when no data is found for the ticker."""
    # Mock yfinance.download to return an empty DataFrame
    mocker.patch('yfinance.download', return_value=pd.DataFrame())

    response = client.post('/process', data={
        'ticker': 'FAKETICKER',
        'start_date': '2023-01-01',
        'end_date': '2023-01-31'
    })

    assert response.status_code == 400
    json_data = response.get_json()
    assert 'No data found' in json_data['error']