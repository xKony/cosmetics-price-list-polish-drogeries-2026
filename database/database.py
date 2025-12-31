import sqlite3


class PriceDatabase:
    def __init__(self, db_name="prices.db"):
        self.db_name = db_name
        self.init_db()

    def _get_connection(self):
        # Enabling foreign_keys is crucial in SQLite
        conn = sqlite3.connect(self.db_name)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def init_db(self):
        """Creates the tables if they don't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Products Dictionary Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ean TEXT UNIQUE,
                    brand TEXT,
                    name TEXT,
                    category TEXT,
                    base_unit TEXT,
                    base_volume REAL
                )
            """
            )

            # Price Facts Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS price_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER,
                    shop_name TEXT,
                    raw_price REAL,
                    effective_price REAL,
                    promo_desc TEXT,
                    is_promo INTEGER, -- SQLite uses 0/1 for BOOLEAN
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products(product_id)
                )
            """
            )
            conn.commit()

    def add_product(self, ean, brand, name, category, unit, volume):
        """Inserts a new product and returns its ID."""
        sql = """INSERT INTO products (ean, brand, name, category, base_unit, base_volume)
                 VALUES (?, ?, ?, ?, ?, ?)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (ean, brand, name, category, unit, volume))
                return cursor.lastrowid
            except sqlite3.IntegrityError:
                # Handle case where EAN already exists
                cursor.execute("SELECT product_id FROM products WHERE ean = ?", (ean,))
                return cursor.fetchone()[0]

    def log_price(self, product_id, shop, raw, effective, desc, is_promo):
        """Logs a price entry."""
        sql = """INSERT INTO price_log (product_id, shop_name, raw_price, effective_price, promo_desc, is_promo)
                 VALUES (?, ?, ?, ?, ?, ?)"""
        with self._get_connection() as conn:
            conn.execute(
                sql, (product_id, shop, raw, effective, desc, 1 if is_promo else 0)
            )
