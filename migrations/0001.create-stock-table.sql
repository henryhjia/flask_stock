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