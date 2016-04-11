from __future__ import print_function

from couchbase.tests.base import CouchbaseTestCase
import couchbase.fulltext as cbft


class FTStringsTest(CouchbaseTestCase):
    def test_fuzzy(self):
        q = cbft.FuzzyQuery('someterm', field='field', boost=1.5,
                            prefix_length=23, fuzziness=12)
        p = cbft.Params(explain=True)

        exp_json = {
            'query': {
                'term': 'someterm',
                'boost': 1.5,
                'fuzziness':  12,
                'prefix_length': 23,
                'field': 'field'
            },
            'indexName': 'someIndex',
            'explain': True
        }

        self.assertEqual(exp_json, cbft.make_search_body('someIndex', q, p))

    def test_match_phrase(self):
        exp_json = {
            'query': {
                'match_phrase': 'salty beers',
                'analyzer': 'analyzer',
                'boost': 1.5,
                'field': 'field'
            },
            'size': 10,
            'indexName': 'ix'
        }

        p = cbft.Params(limit=10)
        q = cbft.MatchPhraseQuery('salty beers', boost=1.5, analyzer='analyzer',
                                  field='field')
        self.assertEqual(exp_json, cbft.make_search_body('ix', q, p))

    def test_match_query(self):
        exp_json = {
            'query': {
                'match': 'salty beers',
                'analyzer': 'analyzer',
                'boost': 1.5,
                'field': 'field',
                'fuzziness': 1234,
                'prefix_length': 4
            },
            'size': 10,
            'indexName': 'ix'
        }

        q = cbft.MatchQuery('salty beers', boost=1.5, analyzer='analyzer',
                            field='field', fuzziness=1234, prefix_length=4)
        p = cbft.Params(limit=10)
        self.assertEqual(exp_json, cbft.make_search_body('ix', q, p))

    def test_string_query(self):
        exp_json = {
            'query': {
                'query': 'q*ry',
                'boost': 2.0,
            },
            'explain': True,
            'size': 10,
            'indexName': 'ix'
        }
        q = cbft.StringQuery('q*ry', boost=2.0)
        p = cbft.Params(limit=10, explain=True)
        self.assertEqual(exp_json, cbft.make_search_body('ix', q, p))

    def test_params(self):
        self.assertEqual({}, cbft.Params().encodable)
        self.assertEqual({'size': 10}, cbft.Params(limit=10).encodable)
        self.assertEqual({'from': 100}, cbft.Params(skip=100).encodable)

        self.assertEqual({'explain': True},
                         cbft.Params(explain=True).encodable)

        self.assertEqual({'highlight': {'style': 'html'}},
                         cbft.Params(highlight_style='html').encodable)

        self.assertEqual({'highlight': {'style': 'ansi',
                                        'fields': ['foo', 'bar', 'baz']}},
                         cbft.Params(highlight_style='ansi',
                                     highlight_fields=['foo', 'bar', 'baz'])
                         .encodable)

        self.assertEqual({'fields': ['foo', 'bar', 'baz']},
                         cbft.Params(fields=['foo', 'bar', 'baz']).encodable)

        p = cbft.Params(facets={
            'term': cbft.TermFacet('somefield', limit=10),
            'dr': cbft.DateFacet('datefield').add_range('name', 'start', 'end'),
            'nr': cbft.NumericFacet('numfield').add_range('name2', 0.0, 99.99)
        })
        exp = {
            'facets': {
                'term': {
                    'field': 'somefield',
                    'size': 10
                },
                'dr': {
                    'field': 'datefield',
                    'date_ranges': [{
                        'name': 'name',
                        'start': 'start',
                        'end': 'end'
                    }]
                },
                'nr': {
                    'field': 'numfield',
                    'numeric_ranges': [{
                        'name': 'name2',
                        'min': 0.0,
                        'max': 99.99
                    }]
                },
            }
        }
        self.assertEqual(exp, p.encodable)