import pandas as pd

#lire le fichier
df = pd.read_csv('flights_final.csv')

# Créer un dossier pour stocker les fichiers de sortie
import os
output_dir = input('Entrez le nom du dossier de sortie: ')
os.makedirs(output_dir, exist_ok=True)


# Grouper les données par la colonne 'timeline'
grouped = df.groupby('timeline')

# Pour chaque groupe de la colonne 'timeline', créer un fichier CSV
for i, (timeline, group) in enumerate(grouped, start=1):
    # Nom du fichier de sortie
    output_file = os.path.join(output_dir, f'vole_{i}.csv')
    # Sauvegarder le groupe dans un fichier CSV
    group.to_csv(output_file, index=False)
    print(f'Fichier {output_file} créé avec {len(group)} lignes.')
