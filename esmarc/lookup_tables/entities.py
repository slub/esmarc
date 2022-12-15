map_entities = {
    "p": "persons",  # Personen, individualisiert
    "n": "persons",  # Personen, namen, nicht individualisiert
    "s": "topics",  # Schlagwörter/Berufe
    "b": "organizations",  # Organisationen
    "g": "geo",  # Geographika
    "u": "works",  # Werktiteldaten
    "f": "events"
}

map_types = {
    "p": "Person",  # Personen, individualisiert
    "n": "Person",  # Personen, namen, nicht individualisiert
    "s": "Thing",  # Schlagwörter/Berufe
    "b": "Organization",  # Organisationen
    "g": "Place",  # Geographika
    "u": "CreativeWork",  # Werktiteldaten
    "f": "Event"
}

map_types_mentions = {
    "p": "Person",  # Personen, individualisiert
    "n": "Person",  # Personen, namen, nicht individualisiert
    "s": "Subject",  # Schlagwörter/Berufe
    "b": "Organization",  # Organisationen
    "g": "GeograficSubject",  # Geographika
    "u": "Works",  # Werktiteldaten
    "f": "Event"
}

map_fields = {"600": {"@type": "Persons",
                      "@id": "persons"},
              "610": {"@type": "Organisation", # (nur wenn nicht 610.* $c und $d)
                      "@id": "organizations"},
              "611": {"@type": "Event", # (oder wenn 610.* $c und $d)
                      "@id": "events"},
              "630": {"@type": "Works",
                      "@id": "works"},
              "648": {"@type": "ChronologicalSubject"},
              "650": {"@type": "Subject",
                      "@id": "topics"},
              "651": {"@type": "GeograficSubject",
                      "@id": "geo"},
              "653": {"@type": "Subject"},
              "655": {"@type": "Genre",
                      "@id": "topics"}
}
