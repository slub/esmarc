from esmarc.marc import getmarc


def getgenre(record, regex, entity):
    """
    gets the genre and builds a schema.org/genre node out of it
    """
    genre = getmarc(record, regex, entity)
    if genre:
        return {"@type": "Text",
                "Text": genre}
