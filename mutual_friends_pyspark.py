from pyspark.sql import SparkSession



def main(input_path, user_a, user_b):
    # Initialiser SparkSession
    spark = SparkSession.builder .appName("MutualFriendsSpark") .getOrCreate()
    sc = spark.sparkContext

    # Charger les données
    # Format attendu par ligne: <user_id> <Nom> <friend_id1>,<friend_id2>,...
    rdd = sc.textFile(input_path)

    # Parser chaque ligne
    parsed = rdd.map(lambda line: line.strip().split()) \
                .filter(lambda parts: len(parts) >= 3) \
                .map(lambda parts: (parts[0], (parts[1], parts[2].split(','))))

    # Construire un dictionnaire user_id -> nom
    id_to_name = dict(parsed.map(lambda x: (x[0], x[1][0])).collect())

    # Générer tous les couples d'amis (min, max) pour chaque relation
    # Émettre ((u,v), liste_amis_u) pour chaque ami v de u
    pairs = parsed.flatMap(lambda x: [(((x[0], friend) if x[0] < friend else (friend, x[0])), x[1][1]) \
                                       for friend in x[1][1]])

    # Réduire par clé pour obtenir l'intersection des listes d'amis
    common = pairs.reduceByKey(lambda l1, l2: list(set(l1).intersection(set(l2))))

    # Normaliser la paire recherchée
    key = tuple(sorted([user_a, user_b]))

    # Filtrer pour la paire et formater le résultat
    result = common.filter(lambda x: x[0] == key) \
                   .map(lambda x: (f"{x[0][0]}{id_to_name[x[0][0]]}{x[0][1]}{id_to_name[x[0][1]]}", ",".join(x[1])))

    # Sauvegarde du résultat
    # Format: 1Nom12Nom2 liste_amis_communs
    result.map(lambda x: f"{x[0]} {x[1]}") \
          .saveAsTextFile("output/mutual_friends")

    spark.stop()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: mutual_friends_pyspark.py <input_path> <userA_id> <userB_id>")
        sys.exit(1)

    input_path = sys.argv[1]
    user_a = sys.argv[2]
    user_b = sys.argv[3]
    main(input_path, user_a, user_b)
