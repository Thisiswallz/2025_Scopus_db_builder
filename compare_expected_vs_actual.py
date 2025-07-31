#!/usr/bin/env python3
"""
Compare expected vs actual publication counts by year
"""

# Expected counts from Scopus query
expected_counts = {
    2025: 6597,
    2024: 8688, 
    2023: 7619,
    2022: 6964,
    2021: 5801,
    2020: 4990,
    2019: 4007,
    2018: 3150,
    2017: 2288
}

# Actual counts from our database
actual_counts = {
    2025: 1187,
    2024: 3203,
    2023: 7608,
    2022: 6951,
    2021: 5776,
    2020: 4982,
    2019: 3996,
    2018: 3140,
    2017: 2270,
    2016: 1708  # This year appears in DB but not expected
}

print("📊 EXPECTED vs ACTUAL PUBLICATION COUNTS")
print("=" * 70)
print(f"{'Year':<6} {'Expected':<10} {'Actual':<10} {'Missing':<10} {'% Found':<10}")
print("-" * 70)

total_expected = 0
total_actual = 0
total_missing = 0

for year in sorted(expected_counts.keys(), reverse=True):
    expected = expected_counts[year]
    actual = actual_counts.get(year, 0)
    missing = expected - actual
    percent_found = (actual / expected * 100) if expected > 0 else 0
    
    total_expected += expected
    total_actual += actual
    total_missing += missing
    
    print(f"{year:<6} {expected:<10,} {actual:<10,} {missing:<10,} {percent_found:<9.1f}%")

# Handle years only in actual (2016)
if 2016 in actual_counts:
    print(f"2016   {'N/A':<10} {actual_counts[2016]:<10,} {'N/A':<10} {'N/A'}")
    total_actual += actual_counts[2016]

print("-" * 70)
print(f"{'TOTAL':<6} {total_expected:<10,} {total_actual:<10,} {total_missing:<10,} {total_actual/total_expected*100:<9.1f}%")

print(f"\n🔍 ANALYSIS BY YEAR:")
print("=" * 50)

for year in sorted(expected_counts.keys(), reverse=True):
    expected = expected_counts[year]
    actual = actual_counts.get(year, 0)
    missing = expected - actual
    percent_found = (actual / expected * 100) if expected > 0 else 0
    
    if percent_found < 90:
        print(f"🚨 {year}: SIGNIFICANT MISSING DATA")
        print(f"   Expected: {expected:,} | Found: {actual:,} | Missing: {missing:,} ({100-percent_found:.1f}% missing)")
    elif percent_found < 98:
        print(f"⚠️  {year}: Minor data gaps")
        print(f"   Expected: {expected:,} | Found: {actual:,} | Missing: {missing:,} ({100-percent_found:.1f}% missing)")
    else:
        print(f"✅ {year}: Nearly complete data ({percent_found:.1f}% found)")

print(f"\n📈 SUMMARY:")
print(f"Total expected documents: {sum(expected_counts.values()):,}")
print(f"Total documents in database: {sum(actual_counts.values()):,}")
print(f"Overall coverage: {total_actual/total_expected*100:.1f}%")
print(f"Missing documents: {total_missing:,}")

# Identify the most problematic years
worst_years = []
for year in expected_counts:
    expected = expected_counts[year]
    actual = actual_counts.get(year, 0)
    percent_found = (actual / expected * 100) if expected > 0 else 0
    if percent_found < 90:
        worst_years.append((year, percent_found, expected - actual))

if worst_years:
    print(f"\n🎯 PRIORITY YEARS FOR INVESTIGATION:")
    worst_years.sort(key=lambda x: x[1])  # Sort by percent found (lowest first)
    for year, percent, missing in worst_years:
        print(f"   {year}: {percent:.1f}% found ({missing:,} missing documents)")