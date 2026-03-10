"""Check overflow data availability in agglomerations"""
import csv
import os

csv_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current', 'agglomerations.csv')

with open(csv_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Check data availability
has_overflow_pe = sum(1 for r in rows if r.get('aggSewerOverflows_pe') and r['aggSewerOverflows_pe'] not in ['', 'NULL'])
has_overflow_m3 = sum(1 for r in rows if r.get('aggSewerOverflows_m3') and r['aggSewerOverflows_m3'] not in ['', 'NULL'])
has_generated = sum(1 for r in rows if r.get('aggGenerated') and r['aggGenerated'] not in ['', 'NULL'])
has_capacity = sum(1 for r in rows if r.get('aggCapacity') and r['aggCapacity'] not in ['', 'NULL'])

print(f'Total agglomerations: {len(rows)}')
print(f'Has aggSewerOverflows_pe: {has_overflow_pe}')
print(f'Has aggSewerOverflows_m3: {has_overflow_m3}')
print(f'Has aggGenerated: {has_generated}')
print(f'Has aggCapacity: {has_capacity}')

# Count by capacity range
cap_100k_plus = 0
cap_10k_100k = 0
cap_under_10k = 0

for r in rows:
    gen = r.get('aggGenerated', '')
    if gen and gen not in ['', 'NULL']:
        try:
            g = float(gen)
            if g >= 100000:
                cap_100k_plus += 1
            elif g >= 10000:
                cap_10k_100k += 1
            else:
                cap_under_10k += 1
        except:
            pass

print(f'\nBy capacity (aggGenerated):')
print(f'  >= 100,000 PE: {cap_100k_plus}')
print(f'  10,000 - 100,000 PE: {cap_10k_100k}')
print(f'  < 10,000 PE: {cap_under_10k}')

# Check 2% calculation feasibility
print('\n--- 2% Overflow Ratio Check ---')
over_2pct = 0
under_2pct = 0
no_data = 0
samples_over = []

for r in rows:
    overflow_pe = r.get('aggSewerOverflows_pe', '')
    generated = r.get('aggGenerated', '')
    
    if not overflow_pe or overflow_pe in ['', 'NULL']:
        no_data += 1
        continue
    if not generated or generated in ['', 'NULL']:
        no_data += 1
        continue
    
    try:
        o = float(overflow_pe)
        g = float(generated)
        if g > 0:
            ratio = (o / g) * 100
            if ratio > 2:
                over_2pct += 1
                if len(samples_over) < 5:
                    samples_over.append((r.get('aggName', '')[:35], g, o, ratio))
            else:
                under_2pct += 1
    except:
        no_data += 1

print(f'Overflow > 2% of generated: {over_2pct}')
print(f'Overflow <= 2% of generated: {under_2pct}')
print(f'No data / invalid: {no_data}')

if samples_over:
    print('\nSample agglomerations with overflow > 2%:')
    for name, gen, overflow, ratio in samples_over:
        print(f'  {name}: {gen:.0f} PE, overflow={overflow:.0f} PE ({ratio:.1f}%)')
