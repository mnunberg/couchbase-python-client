from couchbase.exceptions import CouchbaseError
from couchbase.views.iterator import AlreadyQueriedError
from couchbase import _to_json
from couchbase._pyport import unicode


def _genprop(converter, *apipaths):
    def fget(self):
        d = self._json_
        try:
            for x in apipaths:
                d = d[x]
            return d
        except KeyError:
            return None

    def fset(self, value):
        value = converter(value)
        d = self._json_
        for x in apipaths[:-1]:
            d = d.setdefault(x, {})
        d[apipaths[-1]] = value

    def fdel(self):
        d = self._json_
        try:
            for x in apipaths[:-1]:
                d = d[x]
            del d[apipaths[-1]]
        except KeyError:
            pass

    return property(fget, fset, fdel)


def _genprop_str(*apipaths):
    return _genprop(unicode, *apipaths)


def _highlight(fmt):
    if fmt not in ('html', 'ansi'):
        raise ValueError(
            'Highlight must be "html" or "ansi", got {0}'.format(fmt))
    return fmt


def _assign_kwargs(self, kwargs):
    for k in kwargs:
        if not hasattr(self, k):
            raise AttributeError(k, 'Not valid for', self.__class__.__name__)
        setattr(self, k, kwargs[k])


ALL_FIELDS = object()


class _Facet(object):
    def __init__(self, field):
        self._json_ = {'field': field}

    @property
    def encodable(self):
        return self._json_

    field = _genprop_str('field')

    def __repr__(self):
        return '{0.__class__.__name__}<{0._json_!r}>'.format(self)


class TermFacet(_Facet):
    def __init__(self, field, limit=0):
        super(TermFacet, self).__init__(field)
        if limit:
            self.limit = limit

    limit = _genprop(int, 'size')


def _mk_range_bucket(name, n1, n2, r1, r2):
    d = {}
    if r1 is not None:
        d[n1] = r1
    if r2 is not None:
        d[n2] = r2
    if not d:
        raise ValueError('Must specify at least one range boundary!')
    d['name'] = name
    return d


class DateFacet(_Facet):
    def __init__(self, field):
        super(DateFacet, self).__init__(field)
        self._ranges = []

    def add_range(self, name, start=None, end=None):
        self._ranges.append(_mk_range_bucket(name, 'start', 'end', start, end))
        return self

    _ranges = _genprop(list, 'date_ranges')


class NumericFacet(_Facet):
    def __init__(self, field):
        super(NumericFacet, self).__init__(field)
        self._ranges = []

    def add_range(self, name, min=None, max=None):
        self._ranges.append(_mk_range_bucket(name, 'min', 'max', min, max))
        return self

    _ranges = _genprop(list, 'numeric_ranges')


class _FacetDict(dict):
    def __setitem__(self, key, value):
        if not isinstance(value, _Facet):
            raise ValueError('Can only add facet')
        if hasattr(value, '_ranges') and not getattr(value, '_ranges'):
            raise ValueError('{} object must have at least one range. Use '
                             'add_range'.format(value.__class__.__name__))
        super(_FacetDict, self).__setitem__(key, value)


class Params(object):
    def __init__(self, **kwargs):
        self._json_ = {}
        self.facets = _FacetDict(**kwargs.pop('facets', {}))
        _assign_kwargs(self, kwargs)

    @property
    def encodable(self):
        if self.facets:
            self._json_['facets'] = {
                n: x.encodable for n, x in self.facets.items()
            }

        return self._json_

    limit = _genprop(int, 'size')
    skip = _genprop(int, 'from')
    explain = _genprop(bool, 'explain')
    fields = _genprop(list, 'fields')
    timeout = _genprop(lambda x: int(x * 1000), 'ctl', 'timeout')
    highlight_style = _genprop(_highlight, 'highlight', 'style')
    highlight_fields = _genprop(list, 'highlight', 'fields')


class Query(object):
    def __init__(self):
        self._json_ = {}

    boost = _genprop(float, 'boost')

    @property
    def encodable(self):
        return self._json_


class RawQuery(Query):
    def __init__(self, obj):
        super(RawQuery, self).__init__()
        self._json_ = obj


class _QMeta(type):
    def __new__(mcs, name, bases, dict):
        if '__init__' not in dict:
            def initfn(self, term, **kwargs):
                bases[0].__init__(self, term, **kwargs)
            dict['__init__'] = initfn
        return super(_QMeta, mcs).__new__(mcs, name, bases, dict)


class _SingleQuery(Query):
    _TERMFIELD = []
    __metaclass__ = _QMeta

    def __init__(self, term, **kwargs):
        super(_SingleQuery, self).__init__()
        self._json_[self._TERMFIELD] = term
        _assign_kwargs(self, kwargs)


class StringQuery(_SingleQuery):
    _TERMFIELD = 'query'
    query = _genprop_str(_TERMFIELD)


class MatchQuery(_SingleQuery):
    _TERMFIELD = 'match'
    match = _genprop_str(_TERMFIELD)
    prefix_length = _genprop(int, 'prefix_length')
    fuzziness = _genprop(int, 'fuzziness')
    field = _genprop_str('field')
    analyzer = _genprop_str('analyzer')


class FuzzyQuery(_SingleQuery):
    _TERMFIELD = 'term'
    fuzziness = _genprop(int, 'fuzziness')
    prefix_length = _genprop(int, 'prefix_length')
    field = _genprop_str('field')


class MatchPhraseQuery(_SingleQuery):
    _TERMFIELD = 'match_phrase'
    match_phrase = _genprop_str(_TERMFIELD)
    field = _genprop_str('field')
    analyzer = _genprop_str('analyzer')


class PrefixQuery(_SingleQuery):
    _TERMFIELD = 'prefix'
    field = _genprop_str('field')
    prefix = _genprop_str(_TERMFIELD)


class RegexQuery(_SingleQuery):
    _TERMFIELD = 'regexp'
    field = _genprop_str('field')
    regex = _genprop_str(_TERMFIELD)


class _RangeQuery(Query):
    def __init__(self, **kwargs):
        super(_RangeQuery, self).__init__()
        _assign_kwargs(self, kwargs)


class NumericRangeQuery(_RangeQuery):
    def __init__(self, range_, **kwargs):
        super(NumericRangeQuery, self).__init__(**kwargs)
        self.min, self.max = range_

    min = _genprop(int, 'min')
    min_inclusive = _genprop(bool, 'min_inclusive')
    max = _genprop(int, 'max')
    max_inclusive = _genprop(bool, 'max_inclusive')
    field = _genprop_str('field')


class DateRangeQuery(_RangeQuery):
    def __init__(self, range_, **kwargs):
        self.start, self.end = range_
        super(DateRangeQuery, self).__init__(**kwargs)

    start = _genprop_str('start')
    end = _genprop_str('end')
    start_inclusive = _genprop(bool, 'start_inclusive')
    end_inclusive = _genprop(bool, 'end_inclusive')
    field = _genprop_str('field')
    date_time_parser = _genprop_str('datetime_parser')


class _CompoundQuery(Query):
    _COMPOUND_FIELDS = []

    def __init__(self, **kwargs):
        super(_CompoundQuery, self).__init__()
        _assign_kwargs(self, kwargs)

    @property
    def encodable(self):
        js = self._json_.copy()
        for src, tgt in self._COMPOUND_FIELDS:
            objs = getattr(self, src)
            if not objs:
                continue
            js[tgt] = [q.encodable for q in objs]
        return js


class ConjunctionQuery(_CompoundQuery):
    _COMPOUND_FIELDS = (('conjuncts',) * 2,)

    def __init__(self, *queries):
        super(ConjunctionQuery, self).__init__()
        self.conjuncts = list(queries)


class DisjunctionQuery(_CompoundQuery):
    _COMPOUND_FIELDS = (('disjuncts',) * 2,)

    def __init__(self, *queries, **kwargs):
        super(DisjunctionQuery, self).__init__()
        _assign_kwargs(self, kwargs)
        self.disjuncts = list(queries)

    min = _genprop(int, 'min')


def _bprop_wrap(name, reqtype):
    def fget(self):
        return self._subqueries.get(name)

    def fset(self, value):
        if value is None:
            if name in self._subqueries:
                del self._subqueries[name]
        elif isinstance(value, reqtype):
            self._subqueries[name] = value
        elif isinstance(value, Query):
            self._subqueries[name] = reqtype(value)
        else:
            try:
                it = iter(value)
            except ValueError:
                raise ValueError('Value must be instance of Query')

            l = []
            for q in it:
                if not isinstance(q, Query):
                    raise ValueError('Item is not a query!', q)
                l.append(q)
            self._subqueries = reqtype(*l)

    def fdel(self):
        setattr(self, name, None)

    return property(fget, fset, fdel)


class BooleanQuery(Query):
    def __init__(self, must=None, should=None, must_not=None):
        super(BooleanQuery, self).__init__()
        self._subqueries = {}
        self.must = must
        self.should = should
        self.must_not = must_not

    must = _bprop_wrap('must', ConjunctionQuery)
    must_not = _bprop_wrap('must_not', DisjunctionQuery)
    should = _bprop_wrap('should', DisjunctionQuery)

    @property
    def encodable(self):
        for src, tgt in ((self.must, 'must'),
                         (self.must_not, 'must_not'),
                         (self.should, 'should')):
            if src:
                self._json_[tgt] = src.encodable

        print self._json_
        return self._json_


class SearchError(CouchbaseError):
    pass


def make_search_body(index, query, params=None):
    """
    Generates a dictionary suitable for encoding as the search body
    :param index: The index name to query
    :param query: The query itself
    :param params: Modifiers for the query
    :return: A dictionary suitable for serialization
    """
    dd = {}

    if not isinstance(query, Query):
        query = StringQuery(query)

    dd['query'] = query.encodable
    if params:
        dd.update(params.encodable)
    dd['indexName'] = index
    return dd


class SearchRequest(object):
    def __init__(self, body, parent, row_factory=lambda x: x):
        """
        Object representing the execution of the request on the
        server.

        .. warning::

            You should typically not call this constructor by
            yourself, rather use the :meth:`~.Bucket.fts_query`
            method (or one of its async derivatives).

        :param params: An :class:`N1QLQuery` object.
        :param parent: The parent :class:`~.couchbase.bucket.Bucket` object
        :param row_factory: Callable which accepts the raw dictionary
            of each row, and can wrap them in a customized class.
            The default is simply to return the dictionary itself.

        To actually receive results of the query, iterate over this
        object.
        """
        self._body = _to_json(body)
        self._parent = parent
        self.row_factory = row_factory
        self.errors = []
        self._mres = None
        self._do_iter = True
        self.__raw = False
        self.__meta_received = False

    def _start(self):
        if self._mres:
            return

        self._mres = self._parent._fts_query(self._body)
        self.__raw = self._mres[None]

    @property
    def raw(self):
        return self.__raw

    @property
    def meta(self):
        """
        Get metadata from the query itself. This is guaranteed to only
        return a Python dictionary.

        Note that if the query failed, the metadata might not be in JSON
        format, in which case there may be additional, non-JSON data
        which can be retrieved using the following

        ::

            raw_meta = req.raw.value

        :return: A dictionary containing the query metadata
        """
        if not self.__meta_received:
            raise RuntimeError(
                'This property only valid once all rows are received!')

        if isinstance(self.raw.value, dict):
            return self.raw.value
        return {}

    @property
    def total_hits(self):
        return self.meta['total_hits']

    @property
    def took(self):
        return self.meta['took']

    @property
    def max_score(self):
        return self.meta['max_score']

    @property
    def facets(self):
        return self.meta['facets']

    def _clear(self):
        del self._parent
        del self._mres

    def _handle_meta(self, value):
        self.__meta_received = True
        if not isinstance(value, dict):
            return
        if 'errors' in value:
            for err in value['errors']:
                raise SearchError.pyexc('N1QL Execution failed', err)

    def _process_payload(self, rows):
        if rows:
            return [self.row_factory(row) for row in rows]

        elif self.raw.done:
            self._handle_meta(self.raw.value)
            self._do_iter = False
            return []
        else:
            # We can only get here if another concurrent query broke out the
            # event loop before we did.
            return []

    def __iter__(self):
        if not self._do_iter:
            raise AlreadyQueriedError()

        self._start()
        while self._do_iter:
            raw_rows = self.raw.fetch(self._mres)
            for row in self._process_payload(raw_rows):
                yield row

    def __repr__(self):
        return ('<{0.__class__.__name__} '
                'body={0._body!r} '
                'response={0.raw.value!r}>'.format(self))
