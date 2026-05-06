"""
A lot of this is copied from Sparv and modified
"""

from enum import StrEnum, auto
import functools
from karppipeline.models import InferredField


class Upos(StrEnum):
    """
    Universal POS tags - UD version 2
    """

    @staticmethod
    def _generate_next_value_(
        name: str,
        start: int,
        count: int,
        last_values: list[str],
    ) -> str:
        return name.upper()

    ADJ = auto()
    ADP = auto()
    ADV = auto()
    AUX = auto()
    CCONJ = auto()
    DET = auto()
    INTJ = auto()
    NOUN = auto()
    NUM = auto()
    PART = auto()
    PRON = auto()
    PROPN = auto()
    PUNCT = auto()
    SCONJ = auto()
    SYM = auto()
    VERB = auto()
    X = auto()


UD_FALLBACK = Upos.X


"""
The point of all the _update_schema functions are to inform the pipeline
about the max length of the field, which can affect results of running
"""


def saldo_to_ud_update_schema(field: InferredField) -> InferredField:
    field.extra["length"] = 5
    return field


@functools.cache
def saldo_to_ud(_, pos: str) -> str:
    return suc_to_ud(None, saldo_to_suc(None, pos))


def saldo_to_suc_update_schema(field: InferredField) -> InferredField:
    field.extra["length"] = 2
    return field


@functools.cache
def saldo_to_suc(_, pos: str) -> str:
    return _saldo_pos_to_suc[pos]


def suc_to_ud_update_schema(field: InferredField) -> InferredField:
    field.extra["length"] = 5
    return field


@functools.cache
def suc_to_ud(_, pos: str) -> str:
    """
    Convert SUC tags to UPOS.

    Args:
        pos: SUC tag.

    Returns:
        UPOS tag.
    """
    pos_dict = {
        "NN": Upos.NOUN,
        "PM": Upos.PROPN,
        "VB": Upos.VERB,  # "AUX" ?
        "IE": Upos.PART,
        "PC": Upos.VERB,  # No ADJ?
        "PL": Upos.PART,  # No ADV, ADP?
        "PN": Upos.PRON,
        "PS": Upos.DET,  # No PRON?
        "HP": Upos.PRON,
        "HS": Upos.DET,  # No PRON?
        "DT": Upos.DET,
        "HD": Upos.DET,
        "JJ": Upos.ADJ,
        "AB": Upos.ADV,
        "HA": Upos.ADV,
        "KN": Upos.CCONJ,
        "SN": Upos.SCONJ,
        "PP": Upos.ADP,
        "RG": Upos.NUM,
        "RO": Upos.ADJ,  # ordinal numerals are adjectives
        "IN": Upos.INTJ,
        "UO": Upos.X,
        "MAD": Upos.PUNCT,
        "MID": Upos.PUNCT,
        "PAD": Upos.PUNCT,
    }
    return pos_dict.get(pos.upper(), UD_FALLBACK)


def isof_to_ud_update_schema(field: InferredField) -> InferredField:
    field.extra["length"] = 5
    return field


@functools.cache
def isof_to_ud(_, pos: str) -> str:
    """
    Convert isofs internal markup for POS into ud (experimental)
    """
    return _isof_nyord_to_ud.get(pos, UD_FALLBACK)


def sveak_to_ud_update_schema(field: InferredField) -> InferredField:
    field.extra["length"] = 5
    return field


@functools.cache
def sveak_to_ud(_, pos: str) -> str:
    """
    Convert internal/legacy SveAk POS to ud
    """
    if pos.startswith("subst") or pos == "ssg":
        return Upos.NOUN
    elif pos.startswith("adj"):
        return Upos.ADJ
    elif pos.startswith("pron"):
        return Upos.PRON
    elif pos.startswith("verb"):
        return Upos.VERB
    elif pos.startswith("övrig"):
        return Upos.X
    else:
        raise ValueError(f"Unknown pos {pos}")


def saol15_to_ud_update_schema(field: InferredField) -> InferredField:
    field.extra["length"] = 5
    return field


@functools.cache
def saol15_to_ud(_, pos: str) -> str:
    """
    Convert isofs internal markup for POS into ud (experimental)
    """
    return _saol15_to_ud.get(pos, UD_FALLBACK)


_saldo_pos_to_suc = {
    "nn": "NN",
    "av": "JJ",
    "vb": "VB",
    "pm": "PM",
    "ab": "AB",
    "in": "IN",
    "pp": "PP",
    "pn": "PN",
    "sn": "SN",
    "kn": "KN",
    "ie": "IE",
    "abh": "AB",
    "nnm": "NN",
    "nna": "NN",
    "avh": "JJ",
    "avm": "JJ",
    "ava": "JJ",
    "vbm": "VB",
    "pmm": "PM",
    "abm": "AB",
    "aba": "AB",
    "pnm": "PN",
    "inm": "IN",
    "ppm": "PP",
    "ppa": "PP",
    "knm": "KN",
    "kna": "KN",
    "snm": "SN",
    # nl and nlm in Saldo is numeral and since we do not now if it is ordinal (SUC:RO), use SUC:RG (cardinal)
    "nl": "RG",  # not RO
    "nlm": "RG",  # not RO
    "al": "DT",
    "pma": "PM",
}

_isof_nyord_to_ud = {
    # combined words (klimatbanta, klimatbantare) get X - unknown
    "substantiv": Upos.NOUN,
    "substantiv, förkortning": Upos.NOUN,
    "substantiv, namn/eponym, teleskopord": Upos.NOUN,
    "substantiv, teleskopord": Upos.NOUN,
    "substantiv, räkneord": Upos.NOUN,
    "substantiv, fras/uttryck": Upos.NOUN,
    "substantiv, namn/eponym": Upos.NOUN,
    "namn/eponym, substantiv": Upos.NOUN,
    "interjektion": Upos.INTJ,
    # några av dessa passar nog bättre som PROPN (proper noun)
    "förkortning": Upos.NOUN,
    "adjektiv": Upos.ADJ,
    "adjektiv, teleskopord": Upos.ADJ,
    # one of the words with this value cannot be PART, but both can be ADJ
    "adjektiv, förled/efterled": Upos.ADJ,
    "fras/uttryck, adjektiv": Upos.ADJ,
    "namn/eponym, adjektiv": Upos.ADJ,
    "verb": Upos.VERB,
    "verb, förkortning": Upos.VERB,
    "verb, namn/eponym": Upos.VERB,
    "verb, teleskopord": Upos.VERB,
    "namn/eponym, verb": Upos.VERB,
    "fras/uttryck, interjektion": Upos.INTJ,
    "fras/uttryck, substantiv": Upos.X,
    "substantiv, förled/efterled": Upos.X,
    "förled/efterled": Upos.X,
    "förled/efterled, substantiv": Upos.X,
    "pronomen": Upos.PRON,
    "räkneord": Upos.NUM,
    # usually multi-word expressions
    "fras/uttryck": Upos.X,
    "fras/uttryck, substantiv, adjektiv": Upos.X,
    "substantiv, verb": Upos.X,
    "substantiv, verb, adjektiv": Upos.X,
    "substantiv, verb, fras/uttryck": Upos.X,
    "substantiv, adjektiv": Upos.X,
    "adjektiv, substantiv": Upos.X,
    "adjektiv, substantiv, förled/efterled": Upos.X,
    "adjektiv, verb, substantiv": Upos.X,
    "verb, adjektiv": Upos.X,
    "verb, adjektiv, substantiv": Upos.X,
    "verb, substantiv": Upos.X,
    "verb, substantiv, adjektiv": Upos.X,
    "övrigt": Upos.X,
}

_saol15_to_ud: dict[str, str] = {
    "adjektiv": Upos.ADJ,
    "adjektiviskt slutled": Upos.X,
    "adverb": Upos.ADV,
    "adverbiellt slutled": Upos.X,
    "bestämd artikel": Upos.DET,
    "förled": Upos.X,
    "infinitivmärke": Upos.PART,
    "interjektion": Upos.INTJ,
    "konjunktion": Upos.CCONJ,
    "namn": Upos.PROPN,
    "obestämd artikel": Upos.DET,
    "preposition": Upos.ADP,
    "pronomen": Upos.PRON,
    "räkneord": Upos.NUM,
    "subjunktion": Upos.SCONJ,
    "substantiv": Upos.NOUN,
    "substantiviskt slutled": Upos.X,
    "verb": Upos.VERB,
    "verbalt slutled": Upos.X,
}
