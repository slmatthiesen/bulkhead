insert into vendors (id, name, email, lead_time_days) values
    ('nutra-source',  'NutraSource Ingredients', 'orders@nutrasource.example',  5),
    ('bulk-botanica', 'Bulk Botanica',           'purchasing@bulkbot.example', 12)
on conflict (id) do nothing;

-- Two products short, both from the same vendor, so the run produces exactly one purchase order.
-- That matters for the demo: "the vendor got one order, not two" is a claim an audience can check
-- at a glance, where counting two against three is a claim they have to hold in their head.
--
-- daily_usage is what makes the question answerable. Vitamin C runs out in 2 days, D3 in 4, and
-- B12 -- despite plenty on hand -- in 7, which is the non-obvious one worth the model saying.
insert into inventory (sku, name, category, on_hand, reorder_threshold, reorder_quantity, unit_cost_cents, daily_usage, vendor_id) values
    ('VIT-D3-5000',  'Vitamin D3 5000 IU',      'vitamins',    12, 50, 240,  310,  3, 'nutra-source'),
    ('VIT-C-1000',   'Vitamin C 1000mg',        'vitamins',     8, 40, 200,  185,  4, 'nutra-source'),
    ('VIT-B12-MEth', 'Vitamin B12 Methyl',      'vitamins',   140, 60, 180,  420, 20, 'nutra-source'),
    ('MAG-GLY-400',  'Magnesium Glycinate',     'minerals',    96, 45, 150,  275,  2, 'bulk-botanica'),
    ('OMEGA3-1200',  'Omega-3 1200mg',          'oils',       220, 80, 200,  530,  5, 'bulk-botanica')
on conflict (sku) do update set
    on_hand = excluded.on_hand,
    reorder_threshold = excluded.reorder_threshold,
    daily_usage = excluded.daily_usage;
