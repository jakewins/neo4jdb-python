
# From Nigel Smalls util library
try:
    unicode
except NameError:
    # Python 3
    def ustr(s, encoding="utf-8"):
        if isinstance(s, str):
            return s
        try:
            return s.decode(encoding)
        except AttributeError:
            return str(s)
else:
    # Python 2
    def ustr(s, encoding="utf-8"):
        if isinstance(s, str):
            return s.decode(encoding)
        else:
            return unicode(s)