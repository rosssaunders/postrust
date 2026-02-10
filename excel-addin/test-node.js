#!/usr/bin/env node
/**
 * Test the postrust WASM engine in Node.js.
 * Proves the same engine that runs in the Excel add-in works correctly.
 */

const fs = require('fs');
const path = require('path');

async function main() {
  console.log('═══════════════════════════════════════════════');
  console.log('  postrust WASM Engine Test (Node.js)');
  console.log('═══════════════════════════════════════════════\n');

  // Load WASM
  console.log('Loading WASM engine...');
  const wasmPath = path.join(__dirname, 'pkg', 'postgrust_bg.wasm');
  const jsPath = path.join(__dirname, 'pkg', 'postgrust.js');
  
  // Dynamic import of ES module
  const mod = await import(jsPath);
  const wasmBytes = fs.readFileSync(wasmPath);
  await mod.default(wasmBytes);
  
  const { execute_sql_json } = mod;
  let passed = 0, failed = 0;

  async function sql(query) {
    const json = await execute_sql_json(query);
    const parsed = JSON.parse(json);
    if (!parsed.ok) return { error: parsed.error || 'Unknown error' };
    // Return the last result (for multi-statement queries, the final SELECT)
    const results = parsed.results || [];
    const result = results[results.length - 1];
    if (!result) return { rows: [], columns: [] };
    return result;
  }

  function assert(condition, msg) {
    if (condition) { console.log(`  ✓ ${msg}`); passed++; }
    else { console.log(`  ✗ FAIL: ${msg}`); failed++; }
  }

  // Test 1: Basic SELECT
  console.log('\n🧪 Test 1: Basic SELECT');
  let r = await sql("SELECT 1 AS num, 'hello' AS greeting");
  assert(!r.error, 'No error');
  assert(r.rows && r.rows.length === 1, '1 row returned');
  assert(r.rows[0][0] === '1', 'num = 1');
  assert(r.rows[0][1] === 'hello', 'greeting = hello');

  // Test 2: CREATE TABLE + INSERT (simulates Excel import)
  console.log('\n🧪 Test 2: CREATE TABLE + INSERT (simulates Excel data import)');
  await sql("CREATE TABLE sales (id INT, customer TEXT, product TEXT, amount DOUBLE PRECISION, region TEXT)");
  await sql("INSERT INTO sales VALUES (1, 'Acme Corp', 'Widget A', 15000.00, 'North')");
  await sql("INSERT INTO sales VALUES (2, 'Beta Inc', 'Widget B', 8500.50, 'South')");
  await sql("INSERT INTO sales VALUES (3, 'Acme Corp', 'Widget C', 22000.00, 'North')");
  await sql("INSERT INTO sales VALUES (4, 'Gamma Ltd', 'Widget A', 12000.75, 'East')");
  await sql("INSERT INTO sales VALUES (5, 'Beta Inc', 'Widget A', 9500.00, 'South')");
  await sql("INSERT INTO sales VALUES (6, 'Acme Corp', 'Widget B', 18000.25, 'West')");
  await sql("INSERT INTO sales VALUES (7, 'Delta Co', 'Widget C', 5500.00, 'East')");
  await sql("INSERT INTO sales VALUES (8, 'Gamma Ltd', 'Widget B', 14000.00, 'North')");
  r = await sql("SELECT COUNT(*) AS cnt FROM sales");
  assert(r.rows[0][0] === '8', '8 rows inserted');

  // Test 3: WHERE + ORDER BY
  console.log('\n🧪 Test 3: WHERE + ORDER BY');
  r = await sql("SELECT customer, amount FROM sales WHERE amount > 10000 ORDER BY amount DESC");
  assert(r.rows.length >= 3, `${r.rows.length} sales over 10k`);
  assert(r.rows[0][0] === 'Acme Corp', 'Highest sale is Acme Corp');

  // Test 4: GROUP BY + aggregates
  console.log('\n🧪 Test 4: GROUP BY + Aggregates');
  r = await sql("SELECT customer, COUNT(*) AS orders, SUM(amount) AS total FROM sales GROUP BY customer ORDER BY total DESC");
  assert(r.rows.length === 4, '4 customers');
  assert(r.rows[0][0] === 'Acme Corp', 'Acme Corp has highest total');

  // Test 5: Window functions (common Excel-like analysis)
  console.log('\n🧪 Test 5: Window Functions (running total)');
  r = await sql("SELECT customer, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM sales");
  assert(r.rows.length === 8, '8 rows with running total');

  // Test 6: RANK
  console.log('\n🧪 Test 6: RANK');
  r = await sql("SELECT customer, amount, RANK() OVER (ORDER BY amount DESC) AS rank FROM sales");
  assert(r.rows.length === 8, '8 rows with rank');

  // Test 7: CTE (pivot-like analysis)
  console.log('\n🧪 Test 7: CTE');
  r = await sql(`
    WITH customer_totals AS (
      SELECT customer, SUM(amount) AS total
      FROM sales GROUP BY customer
    )
    SELECT customer, total
    FROM customer_totals
    WHERE total > 20000
    ORDER BY total DESC
  `);
  assert(r.rows.length >= 1, 'At least 1 customer with > 20k total');

  // Test 8: JOIN (multi-table)
  console.log('\n🧪 Test 8: JOIN');
  await sql("CREATE TABLE regions (name TEXT, manager TEXT)");
  await sql("INSERT INTO regions VALUES ('North', 'Alice'), ('South', 'Bob'), ('East', 'Charlie'), ('West', 'Diana')");
  r = await sql("SELECT customer, amount, manager FROM sales JOIN regions ON region = name ORDER BY amount DESC LIMIT 3");
  assert(r.rows.length === 3, 'Top 3 sales with region manager');
  assert(r.columns.length === 3, '3 columns in join result');

  // Test 9: Subquery
  console.log('\n🧪 Test 9: Subquery');
  r = await sql("SELECT customer, amount FROM sales WHERE amount > (SELECT AVG(amount) FROM sales) ORDER BY amount DESC");
  assert(r.rows.length >= 1, 'Above-average sales found');

  // Test 10: JSON
  console.log('\n🧪 Test 10: JSON functions');
  r = await sql(`SELECT json_extract_path_text('{"name": "postrust", "version": "1"}', 'name') AS engine`);
  assert(r.rows[0][0] === 'postrust', 'JSON extraction works');

  // Test 11: String functions
  console.log('\n🧪 Test 11: String functions');
  r = await sql("SELECT UPPER('hello') AS up, LENGTH('world') AS len, CONCAT('foo', 'bar') AS cat");
  assert(r.rows[0][0] === 'HELLO', 'UPPER works');
  assert(r.rows[0][1] === '5', 'LENGTH works');
  assert(r.rows[0][2] === 'foobar', 'CONCAT works');

  // Test 12: Math functions
  console.log('\n🧪 Test 12: Math functions');
  r = await sql("SELECT ABS(-42) AS a, ROUND(3.14159, 2) AS b, POWER(2, 10) AS c");
  assert(r.rows[0][0] === '42', 'ABS works');
  assert(r.rows[0][2].startsWith('1024'), 'POWER works');

  // Summary
  console.log('\n═══════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed`);
  console.log('═══════════════════════════════════════════════\n');
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error(`Fatal: ${err.message}`);
  process.exit(1);
});
