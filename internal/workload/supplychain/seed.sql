insert into vendors (id, name, email, lead_time_days) values
    ('nutra-source',  'NutraSource Ingredients', 'orders@nutrasource.example',  5),
    ('bulk-botanica', 'Bulk Botanica',           'purchasing@bulkbot.example', 12)
on conflict (id) do nothing;

-- Two vitamins below their reorder threshold and one comfortably above, so the agent has to
-- actually decide rather than order everything it sees.
insert into inventory (sku, name, category, on_hand, reorder_threshold, reorder_quantity, unit_cost_cents, vendor_id) values
    ('VIT-D3-5000',  'Vitamin D3 5000 IU',      'vitamins',    12, 50, 240,  310, 'nutra-source'),
    ('VIT-C-1000',   'Vitamin C 1000mg',        'vitamins',     8, 40, 200,  185, 'nutra-source'),
    ('VIT-B12-MEth', 'Vitamin B12 Methyl',      'vitamins',   140, 60, 180,  420, 'nutra-source'),
    ('MAG-GLY-400',  'Magnesium Glycinate',     'minerals',    31, 45, 150,  275, 'bulk-botanica'),
    ('OMEGA3-1200',  'Omega-3 1200mg',          'oils',       220, 80, 200,  530, 'bulk-botanica')
on conflict (sku) do update set
    on_hand = excluded.on_hand,
    reorder_threshold = excluded.reorder_threshold;
