# Postrust CLI Demo
# Records a GIF showing psql connecting to Postrust and running queries

Output assets/demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 600
Set Theme "Dracula"
Set Padding 20
Set TypingSpeed 40ms

# Connect to Postrust
Type "psql -h 127.0.0.1 -p 55432 -U postgres"
Enter
Sleep 1s

# Create a table
Type "CREATE TABLE orders (id SERIAL PRIMARY KEY, item TEXT, qty INT, price NUMERIC(10,2));"
Enter
Sleep 500ms

# Insert data
Type "INSERT INTO orders (item, qty, price) VALUES ('Widget', 100, 9.99), ('Gadget', 50, 24.95), ('Doohickey', 200, 3.50);"
Enter
Sleep 500ms

# Query with aggregation
Type "SELECT item, qty * price AS total FROM orders ORDER BY total DESC;"
Enter
Sleep 1.5s

# Window function
Type "SELECT item, price, RANK() OVER (ORDER BY price DESC) FROM orders;"
Enter
Sleep 1.5s

# JSON
Type "SELECT jsonb_build_object('item', item, 'total', qty * price) AS json_row FROM orders;"
Enter
Sleep 1.5s

# CTE
Type "WITH summary AS (SELECT COUNT(*) AS items, SUM(qty * price) AS revenue FROM orders) SELECT * FROM summary;"
Enter
Sleep 1.5s

# HTTP extension
Type "CREATE EXTENSION http;"
Enter
Sleep 500ms

Type "SELECT content::jsonb->'origin' AS my_ip FROM http_get('https://httpbin.org/ip');"
Enter
Sleep 3s

# Exit
Type "\q"
Enter
Sleep 500ms
