TYPE_OIDS = {
    "bool": 16,
    "int4": 23,
    "int8": 20,
    "numeric": 1700,
    "text": 25,
    "bpchar": 1042,
    "varchar": 1043,
    "date": 1082,
    "timestamptz": 1184,
}

TYPE_NAMES = {oid: name for name, oid in TYPE_OIDS.items()}


def oid_for(name: str) -> int:
    if name not in TYPE_OIDS:
        raise KeyError(
            "no oid registered for postgres type {!r}; add it to TYPE_OIDS".format(name)
        )
    return TYPE_OIDS[name]


def describe(oid: int) -> str:
    return "{} (oid {})".format(TYPE_NAMES.get(oid, "unregistered type"), oid)
