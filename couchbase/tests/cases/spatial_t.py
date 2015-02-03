#
# Copyright 2015, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from couchbase.tests.base import RealServerTestCase
from couchbase.user_constants import FMT_JSON
from couchbase.views.params import SpatialQuery

DESIGN_JSON = {
    'language': 'javascript',
    'spatial': {
        'simpleGeo':
            '''
            function(doc) {
                if (doc.geometry && doc.locname) {
                    emit (doc.geometry, doc.locname);
                }
            }
            '''.replace("\n", '')
    }
}

DOCS_JSON = {
    'mountain-view_ca_usa': {
        'locname': ['Oakland', 'CA', 'USA'],
        'loc': [37, -122]
    },
    'reno_nv_usa': {
        'locname': ['Reno', 'NV', 'USA'],
        'loc': [39, -119]
    },
    'guayaquil_guayas_ec': {
        'locname': ['Guayaquil', 'Guayas', 'Ecuador'],
        'loc': [-2, -79]
    },
    'banos_tungurahua_ec': {
        'locname': ['Banos', 'Tungurahua', 'Ecuador'],
        'loc': [-1, 78]
    }
}


class SpatialTest(RealServerTestCase):
    def setUp(self):
        super(SpatialTest, self).setUp()
        mgr = self.cb.bucket_manager()
        ret = mgr.design_create('geo', DESIGN_JSON, use_devmode=False)
        self.assertTrue(ret.success)
        self.assertTrue(self.cb.upsert_multi(DOCS_JSON, format=FMT_JSON).all_ok)

    def test_simple_spatial(self):
        spq = SpatialQuery()
        for r in self.cb.query('geo', 'simpleGeo', query=spq):
            pass
        #TODO: How do we test this properly?