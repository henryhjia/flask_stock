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
    # This is the real database connection that will hold our test data
    real_conn = sqlite3.connect(":memory:")
    real_conn.row_factory = sqlite3.Row
    real_conn.execute(CREATE_TABLE_SQL)
    real_conn.commit()

    # This is a mock connection object that we can control
    mock_conn = mocker.MagicMock()

    # This factory will be called every time the app asks for a cursor
    def mock_cursor_factory():
        real_cursor = real_conn.cursor()
        mock_cursor = mocker.MagicMock()

        # Implement the 'execute' method for our mock cursor
        def mock_execute(sql, params=()):
            # Translate the SQL parameter style from '''%s''' to '?'
            sql = sql.replace("%s", "?")
            return real_cursor.execute(sql, params)

        # Wire up the mock cursor's methods to the real cursor
        mock_cursor.execute.side_effect = mock_execute
        mock_cursor.fetchone.side_effect = real_cursor.fetchone
        mock_cursor.fetchall.side_effect = real_cursor.fetchall
        mock_cursor.close.side_effect = real_cursor.close
        mock_cursor.__iter__ = real_cursor.__iter__
        
        # Wire up the 'description' attribute
        type(mock_cursor).description = mocker.PropertyMock(side_effect=lambda: real_cursor.description)

        return mock_cursor

    # When the app calls .cursor(), execute our factory to get the patched cursor
    mock_conn.cursor.side_effect = mock_cursor_factory
    
    # Pass through other necessary methods to the real connection
    mock_conn.commit.side_effect = real_conn.commit
    mock_conn.close.side_effect = real_conn.close

    # Patch the get_db_connection function in the app to return our mock connection
    mocker.patch('app.get_db_connection', return_value=mock_conn)

    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

    # Clean up the real connection after the test
    real_conn.close()


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
