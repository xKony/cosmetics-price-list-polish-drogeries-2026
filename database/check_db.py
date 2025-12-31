import sqlite3
import os

def check_database():
    db_path = os.path.join(os.path.dirname(__file__), "prices.db")
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n=== Database Summary ===")
    
    # 1. Row counts
    cursor.execute("SELECT count(*) FROM products")
    product_count = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM price_log")
    log_count = cursor.fetchone()[0]
    print(f"Total Products: {product_count}")
    print(f"Total Price Logs: {log_count}")

    # 2. Latest 15 logs with product details
    print("\n=== Latest 15 Price Logs ===")
    query = """
        SELECT 
            p.ean,
            p.brand, 
            p.name, 
            pl.raw_price, 
            pl.last_30d_price, 
            pl.review_ratings,
            pl.promo_desc,
            pl.scraped_at
        FROM price_log pl
        JOIN products p ON pl.product_id = p.product_id
        ORDER BY pl.scraped_at DESC
        LIMIT 15
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Simple formatting
    print(f"{'EAN/Kod':<12} | {'Brand':<10} | {'Name':<35} | {'Price':<7} | {'Min30d':<7} | {'Stars':<5} | {'Promo'}")
    print("-" * 110)
    for row in rows:
        ean, brand, name, price, min30, stars, promo, _ = row
        # Sanitize name for display
        dn = (name[:32] + '...') if len(str(name)) > 35 else name
        print(f"{str(ean):<12} | {str(brand):<10} | {str(dn):<35} | {str(price):<7} | {str(min30):<7} | {str(stars):<5} | {str(promo)}")

    conn.close()

if __name__ == "__main__":
    try:
        check_database()
    except Exception as e:
        print(f"An error occurred: {e}")
