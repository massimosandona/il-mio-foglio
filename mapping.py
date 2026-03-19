import csv
from collections import Counter

MAPPING = {
    '-':                                                    ('NON CLASSIFICATO','Altro'),
    'Non Classificati':                                     ('NON CLASSIFICATO','Altro'),
    'Alternativi Global Macro EUR':                         ('ALTERNATIVO','Globale'),
    'Alternativi Long/Short Azionario Altro':               ('ALTERNATIVO','Globale'),
    'Alternativi Long/Short Azionario Europa':              ('ALTERNATIVO','Europa'),
    'Alternativi Long/Short Azionario US':                  ('ALTERNATIVO','Nord America'),
    'Alternativi Market Neutral EUR':                       ('ALTERNATIVO','Europa'),
    'Alternativi Multistrategy EUR':                        ('ALTERNATIVO','Globale'),
    'Alternativi Trading Opzioni':                          ('ALTERNATIVO','Globale'),
    'Azionari ASEAN':                                       ('AZIONARIO','Asia Pacifico'),
    'Azionari Altro':                                       ('AZIONARIO','Altro'),
    'Azionari America Latina':                              ('AZIONARIO','Paesi Emergenti'),
    'Azionari Area Euro Large Cap':                         ('AZIONARIO','Europa'),
    'Azionari Area Euro Mid Cap':                           ('AZIONARIO','Europa'),
    'Azionari Asia ex Giappone':                            ('AZIONARIO','Asia Pacifico'),
    'Azionari Asia ex Giappone Small/Mid Cap':              ('AZIONARIO','Asia Pacifico'),
    'Azionari Asia-Pacifico ex Giappone':                   ('AZIONARIO','Asia Pacifico'),
    'Azionari Asia-Pacifico ex Giappone Reddito':           ('AZIONARIO','Asia Pacifico'),
    'Azionari Cina':                                        ('AZIONARIO','Asia Pacifico'),
    'Azionari Cina - A Shares':                             ('AZIONARIO','Asia Pacifico'),
    'Azionari Europa Flex Cap':                             ('AZIONARIO','Europa'),
    'Azionari Europa Large Cap Blend':                      ('AZIONARIO','Europa'),
    'Azionari Europa Large Cap Growth':                     ('AZIONARIO','Europa'),
    'Azionari Europa Large Cap Value':                      ('AZIONARIO','Europa'),
    'Azionari Europa Reddito':                              ('AZIONARIO','Europa'),
    'Azionari Europa Small Cap':                            ('AZIONARIO','Europa'),
    'Azionari Giappone Large Cap':                          ('AZIONARIO','Giappone'),
    'Azionari Giappone Small/Mid Cap':                      ('AZIONARIO','Giappone'),
    'Azionari Globali Reddito':                             ('AZIONARIO','Globale'),
    'Azionari Grande Cina':                                 ('AZIONARIO','Asia Pacifico'),
    'Azionari India':                                       ('AZIONARIO','Asia Pacifico'),
    'Azionari Internazionali - Frontier Markets':           ('AZIONARIO','Paesi Emergenti'),
    'Azionari Internazionali Flex Cap':                     ('AZIONARIO','Globale'),
    'Azionari Internazionali Large Cap Blend':              ('AZIONARIO','Globale'),
    'Azionari Internazionali Large Cap Growth':             ('AZIONARIO','Globale'),
    'Azionari Internazionali Large Cap Value':              ('AZIONARIO','Globale'),
    'Azionari Internazionali Small/Mid Cap':                ('AZIONARIO','Globale'),
    'Azionari Italia':                                      ('AZIONARIO','Europa'),
    'Azionari Pacifico ex Giappone':                        ('AZIONARIO','Asia Pacifico'),
    'Azionari Paesi Emergenti':                             ('AZIONARIO','Paesi Emergenti'),
    'Azionari Paesi Emergenti Small/Mid Cap':               ('AZIONARIO','Paesi Emergenti'),
    'Azionari Settore Acqua':                               ('AZIONARIO','Globale'),
    'Azionari Settore Beni Industriali':                    ('AZIONARIO','Globale'),
    'Azionari Settore Beni e Servizi di Consumo':           ('AZIONARIO','Globale'),
    'Azionari Settore Biotecnologia':                       ('AZIONARIO','Globale'),
    'Azionari Settore Comunicazioni':                       ('AZIONARIO','Globale'),
    'Azionari Settore Ecologia':                            ('AZIONARIO','Globale'),
    'Azionari Settore Energia':                             ('AZIONARIO','Globale'),
    'Azionari Settore Energia Alternativa':                 ('AZIONARIO','Globale'),
    'Azionari Settore Infrastrutture':                      ('AZIONARIO','Globale'),
    'Azionari Settore Metalli Preziosi':                    ('AZIONARIO','Globale'),
    'Azionari Settore Risorse Naturali':                    ('AZIONARIO','Globale'),
    'Azionari Settore Salute':                              ('AZIONARIO','Globale'),
    'Azionari Settore Servizi Finanziari':                  ('AZIONARIO','Globale'),
    'Azionari Settore Tecnologia':                          ('AZIONARIO','Globale'),
    'Azionari Svizzera':                                    ('AZIONARIO','Europa'),
    'Azionari UK Mid Cap':                                  ('AZIONARIO','Europa'),
    'Azionari USA Large Cap Blend':                         ('AZIONARIO','Nord America'),
    'Azionari USA Large Cap Growth':                        ('AZIONARIO','Nord America'),
    'Azionari USA Large Cap Value':                         ('AZIONARIO','Nord America'),
    'Azionari USA Reddito':                                 ('AZIONARIO','Nord America'),
    'Azionari USA Small Cap':                               ('AZIONARIO','Nord America'),
    'Bilanciati Aggressivi EUR - Globali':                  ('BILANCIATO','Globale'),
    'Bilanciati Aggressivi USD':                            ('BILANCIATO','Globale'),
    'Bilanciati Altro':                                     ('BILANCIATO','Altro'),
    'Bilanciati Asia':                                      ('BILANCIATO','Asia Pacifico'),
    'Bilanciati Flessibili EUR - Globali':                  ('BILANCIATO','Globale'),
    'Bilanciati Grande Cina':                               ('BILANCIATO','Asia Pacifico'),
    'Bilanciati Moderati EUR':                              ('BILANCIATO','Europa'),
    'Bilanciati Moderati EUR - Globali':                    ('BILANCIATO','Globale'),
    'Bilanciati Moderati USD':                              ('BILANCIATO','Globale'),
    'Bilanciati Prudenti EUR':                              ('BILANCIATO','Europa'),
    'Bilanciati Prudenti EUR - Globali':                    ('BILANCIATO','Globale'),
    'Immobiliare Indiretto - Globale':                      ('IMMOBILIARE','Globale'),
    'Immobiliare Indiretto - Nord America':                 ('IMMOBILIARE','Nord America'),
    'Immobiliare indiretto - Europa':                       ('IMMOBILIARE','Europa'),
    'Immobiliari Indiretto - Altro':                        ('IMMOBILIARE','Altro'),
    'Materie Prime - Generiche':                            ('MATERIE PRIME','Globale'),
    'Materie Prime - Metalli Preziosi':                     ('MATERIE PRIME','Globale'),
    'Monetari Altro':                                       ('MONETARIO','Altro'),
    'Monetari CHF':                                         ('MONETARIO','Europa'),
    'Monetari EUR':                                         ('MONETARIO','Europa'),
    'Monetari a Breve Termine EUR':                         ('MONETARIO','Europa'),
    'Monetari a Breve Termine USD':                         ('MONETARIO','Nord America'),
    'Obbligazionari Altro':                                 ('OBBLIGAZIONARIO','Altro'),
    'Obbligazionari Asia':                                  ('OBBLIGAZIONARIO','Asia Pacifico'),
    'Obbligazionari Brevissimo Termine EUR':                ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Cina':                                  ('OBBLIGAZIONARIO','Asia Pacifico'),
    'Obbligazionari Convertibili - Altro':                  ('OBBLIGAZIONARIO','Altro'),
    'Obbligazionari Convertibili Europa':                   ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Convertibili Globali - EUR Hedged':     ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Corporate Breve Termine EUR':           ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Corporate EUR':                         ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Corporate Globali':                     ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Corporate Globali - EUR Hedged':        ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Corporate Globali - USD Hedged':        ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Corporate Paesi Emergenti':             ('OBBLIGAZIONARIO','Paesi Emergenti'),
    'Obbligazionari Corporate Paesi Emergenti EUR':         ('OBBLIGAZIONARIO','Paesi Emergenti'),
    'Obbligazionari Corporate USD':                         ('OBBLIGAZIONARIO','Nord America'),
    'Obbligazionari Diversificati Breve Termine EUR':       ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Diversificati Breve Termine USD':       ('OBBLIGAZIONARIO','Nord America'),
    'Obbligazionari Diversificati EUR':                     ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Diversificati USD':                     ('OBBLIGAZIONARIO','Nord America'),
    'Obbligazionari Flessibili EUR':                        ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Flessibili Globali':                    ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Flessibili Globali - EUR Hedged':       ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Flessibili Globali - USD Hedged':       ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Flessibili USD':                        ('OBBLIGAZIONARIO','Nord America'),
    'Obbligazionari Globali':                               ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Globali EUR - Hedged':                  ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Globali USD - Hedged':                  ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Governativi Breve Termine EUR':         ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Governativi Breve Termine USD':         ('OBBLIGAZIONARIO','Nord America'),
    'Obbligazionari Governativi EUR':                       ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Governativi GBP':                       ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Governativi Globali':                   ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Governativi Globali EUR Hedged':        ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Governativi USD':                       ('OBBLIGAZIONARIO','Nord America'),
    'Obbligazionari High Yield EUR':                        ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari High Yield Globali':                    ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari High Yield Globali - EUR Hedged':       ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari High Yield USD':                        ('OBBLIGAZIONARIO','Nord America'),
    'Obbligazionari Inflation-Linked EUR':                  ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Inflation-Linked Globali':              ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Inflation-Linked Globali - EUR Hedged': ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari Inflation-Linked Globali - USD Hedged': ('OBBLIGAZIONARIO','Globale'),
    'Obbligazionari JPY':                                   ('OBBLIGAZIONARIO','Giappone'),
    'Obbligazionari Lungo Termine EUR':                     ('OBBLIGAZIONARIO','Europa'),
    'Obbligazionari Paesi Emergenti':                       ('OBBLIGAZIONARIO','Paesi Emergenti'),
    'Obbligazionari Paesi Emergenti EUR':                   ('OBBLIGAZIONARIO','Paesi Emergenti'),
    'Obbligazionari Paesi Emergenti Valuta Locale':         ('OBBLIGAZIONARIO','Paesi Emergenti'),
    'Obbligazionari RMB - Onshore':                         ('OBBLIGAZIONARIO','Asia Pacifico'),
    'Obbligazionari Subordinati EUR':                       ('OBBLIGAZIONARIO','Europa'),
}

# Read CSV
with open('C:/Progetti/IL MIO FOGLIO/UNIFICATO.csv','r',encoding='utf-8-sig') as f:
    reader = csv.reader(f, delimiter=';')
    header = next(reader)
    rows = list(reader)

header = [h for h in header if h]

# Check coverage
unmapped = set()
for r in rows:
    cat = r[2].strip() if len(r) > 2 else ''
    if cat not in MAPPING:
        unmapped.add(cat)

if unmapped:
    print('CATEGORIE NON MAPPATE:')
    for u in sorted(unmapped):
        print(f'  [{u}]')
else:
    print('Tutte le 133 categorie mappate con successo!')

# Add new columns
header.extend(['Macro Classe', 'Zona Geografica'])
new_rows = []
for r in rows:
    cat = r[2].strip() if len(r) > 2 else ''
    macro, geo = MAPPING.get(cat, ('NON CLASSIFICATO', 'Altro'))
    while len(r) < 8:
        r.append('')
    r.extend([macro, geo])
    new_rows.append(r)

# Write
with open('C:/Progetti/IL MIO FOGLIO/UNIFICATO_v2.csv','w',encoding='utf-8-sig',newline='') as f:
    writer = csv.writer(f, delimiter=';')
    writer.writerow(header)
    writer.writerows(new_rows)

# Stats
macro_count = Counter(r[8] for r in new_rows)
geo_count = Counter(r[9] for r in new_rows)
print()
print('=== MACRO CLASSI ===')
for k, v in macro_count.most_common():
    print(f'  {k}: {v}')
print()
print('=== ZONE GEOGRAFICHE ===')
for k, v in geo_count.most_common():
    print(f'  {k}: {v}')
print()
print(f'CSV aggiornato: {len(header)} colonne, {len(new_rows)} righe')
print('Colonne:', ';'.join(header))
