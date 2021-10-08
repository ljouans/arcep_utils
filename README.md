# arcep_utils

Boîte à outils pour mes projets conduits à l'ARCEP d'Avril à Octobre 2021.
Les outils tombent dans cinq catégories: 
- Les outils de chemin, pour travailler en chemin relatif depuis ce dossier (`pathtools.py`)[./pathtools.py] ;
- Les outils de base données, pour réaliser des requêtes dans une base PostGresQL/PostGIS et renvoyer des Géo/DataFrames (`dbtool.py`)[./dbtools.py] ;
- Les outils de lecture de fichier de configuration, basé sur `configparser` (`config/`)[./config/] ;
- Les outils de lecture d'IPE (`ipe/`)[./ipe/] ;
- Et la corbeille, (`misc.py`)[./misc.py] pour des itérateurs `tqdm` un peu généraux ou pour de l'écriture de fichier.

## Notes spécifiques

Ces outils n'ont pas vraiment de sens dans un autre contexte que celui de l'équipe Unité Couverture Fixe (UCF) à l'ARCEP, sauf peut-être l'outil de chemins.
Vous pouvez néanmoins vous en inspirer librement tant que vous respectez la license.

### Outil de chemin

Les chemins Python sont relatifs par rapport à l'exécutant. 
C'est un problème dès qu'on manipule des chemins relatifs.

Le fichier `pathtools.py` replace la relativité des chemins à partir du fichier, et plus de l'exécutable.

### Outils de BDD

C'est basiquement un wrapper pour SQLAlchemy. 
La difficulté interviens dès que l'on manipule des données géographiques~: la conversion vers GéoPandas nécessite un peu de magie.

### Lecture d'IPE

Les IPE sont des fichiers fourni par les opérateurs de déploiement de la fibre, standardisés par le groupe Interop'Fibre. 
Ce sont, dans notre contexte de travail, une grosse archive pleine de CSV volumineux.
L'outil, ici, parcours l'archive, extrait uniquement les colonnes demandées, et surtout essaye les différents encodages pour plaire à Pandas.
