import sqlite3
import os
import sys

# Force UTF-8 encoding for stdout (prevents char corruption in piped output)
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

def check_database():
    db_path = os.path.join(os.path.dirname(__file__), "prices.db")
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    # Ensure correct unicode handling
    conn.text_factory = str 
    cursor = conn.cursor()

    print("\n=== Database Summary ===")
    
    # 1. Row counts
    cursor.execute("SELECT count(*) FROM products")
    product_count = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM price_log")
    log_count = cursor.fetchone()[0]
    print(f"Total Products: {product_count}")
    print(f"Total Price Logs: {log_count}")

    # 2. Latest 100 logs with ALL details
    print("\n=== Latest 100 Price Logs (All Columns) ===")
    query = """
        SELECT 
            p.product_id,
            p.ean,
            p.brand, 
            p.name, 
            p.category,
            p.base_unit,
            p.base_volume,
            pl.shop_name,
            pl.raw_price, 
            pl.last_30d_price, 
            pl.review_ratings,
            pl.promo_desc,
            pl.is_promo,
            pl.scraped_at
        FROM price_log pl
        JOIN products p ON pl.product_id = p.product_id
        ORDER BY pl.scraped_at DESC
        LIMIT 100
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Header
    header = (
        f"{'ID':<4} | {'EAN':<15} | {'Brand':<15} | {'Name':<30} | {'Cat':<8} | "
        f"{'Unit':<5} | {'Vol':<6} | {'Shop':<8} | {'Price':<7} | {'Min30d':<7} | "
        f"{'Stars':<5} | {'PromoDesc':<25} | {'IsPro':<5} | {'Time'}"
    )
    print(header)
    print("-" * len(header))

    for row in rows:
        pid, ean, brand, name, cat, unit, vol, shop, price, min30, stars, promo, is_promo, time = row
        
        # Sanitize name
        dn = (str(name)[:27] + '...') if len(str(name)) > 30 else str(name)
        # Sanitize promo
        dp = (str(promo)[:22] + '...') if promo and len(str(promo)) > 25 else str(promo)
        
        print(
            f"{str(pid):<4} | {str(ean):<15} | {str(brand):<15} | {dn:<30} | {str(cat):<8} | "
            f"{str(unit):<5} | {str(vol):<6} | {str(shop):<8} | {str(price):<7} | {str(min30):<7} | "
            f"{str(stars):<5} | {dp:<25} | {str(is_promo):<5} | {str(time)}"
        )

    conn.close()

if __name__ == "__main__":
    try:
        check_database()
    except Exception as e:
        print(f"An error occurred: {e}")
