import sqlite3
import pytest
import pandas as pd
from app import app

# SQL to create a schema for the test database
CREATE_TABLE_SQL = """
CREATE TABLE stock (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    It mocks the database connection to use a self-contained, in-memory SQLite
    database, ensuring tests are isolated and don't depend on external services.
    """
    # Use an in-memory SQLite database for testing
    conn = sqlite3.connect(":memory:")
    
    # The app code uses DictCursor, so we set the row_factory
    conn.row_factory = sqlite3.Row
    
    # Create the database schema
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    # The app code uses '''%s''' placeholders, but sqlite3 uses '?'.
    # We need to patch the 'execute' method to translate the queries.
    original_cursor = conn.cursor
    def mock_cursor_factory():
        cursor = original_cursor()
        original_execute = cursor.execute
        def new_execute(sql, params=()):
            sql = sql.replace("%s", "?")
            return original_execute(sql, params)
        cursor.execute = new_execute
        return cursor
    
    conn.cursor = mock_cursor_factory

    # Patch the get_db_connection function in the app to return our mock connection
    mocker.patch('app.get_db_connection', return_value=conn)

    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

    # Clean up the connection after the test
    conn.close()


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