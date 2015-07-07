/**
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/
#include "oputil.h"

static int
sd_convert_spec(PyObject *pyspec, lcb_SDSPEC *sdspec,
    pycbc_pybuffer *pathbuf, pycbc_pybuffer *valbuf)
{
    PyObject *path = NULL;
    PyObject *val = NULL;
    int op = 0, create = 0;

    if (!PyTuple_Check(pyspec)) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Expected tuple for spec", pyspec);
        return -1;
    }

    if (!PyArg_ParseTuple(pyspec, "iO|Oi", &op, &path, &val, &create)) {
        PYCBC_EXCTHROW_ARGS();
        return -1;
    }
    if (pycbc_tc_simple_encode(path, pathbuf, PYCBC_FMT_UTF8) != 0) {
        PYCBC_PYBUF_RELEASE(pathbuf);
        return -1;
    }

    sdspec->sdcmd = op;
    sdspec->options = create ? LCB_SDSPEC_F_MKINTERMEDIATES : 0;
    LCB_SDSPEC_SET_PATH(sdspec, pathbuf->buffer, pathbuf->length);
    if (val != NULL) {
        int rv = pycbc_tc_simple_encode(val, valbuf, PYCBC_FMT_JSON);
        if (rv != 0) {
            PYCBC_PYBUF_RELEASE(pathbuf);
            return -1;
        }
        LCB_SDSPEC_SET_VALUE(sdspec, valbuf->buffer, valbuf->length);
    }
    return 0;
}

int
pycbc_sd_handle_speclist(pycbc_Bucket *self, pycbc_MultiResult *mres,
    PyObject *key, PyObject *spectuple, lcb_CMDSUBDOC *cmd)
{
    int rv;
    lcb_error_t err = LCB_SUCCESS;
    Py_ssize_t nspecs = 0;
    pycbc__SDResult *newitm = NULL;
    lcb_SDSPEC *specs = NULL, spec_s = { 0 };

    if (!PyTuple_Check(spectuple)) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Value must be a tuple!");
        return -1;
    }

    nspecs = PyTuple_GET_SIZE(spectuple);
    if (nspecs == 0) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Need one or more commands!");
        return -1;
    }

    newitm = pycbc_sdresult_new(self, spectuple);
    newitm->key = key;
    Py_INCREF(newitm->key);

    if (nspecs == 1) {
        pycbc_pybuffer pathbuf = { 0 }, valbuf = { 0 };
        PyObject *single_spec = PyTuple_GET_ITEM(spectuple, 0);
        cmd->specs = &spec_s;
        cmd->nspecs = 1;
        rv = sd_convert_spec(single_spec, &spec_s, &pathbuf, &valbuf);
    } else {
        Py_ssize_t ii;
        specs = calloc(nspecs, sizeof *specs);
        cmd->specs = specs;
        cmd->nspecs = nspecs;

        for (ii = 0; ii < nspecs; ++ii) {
            PyObject *cur = PyTuple_GET_ITEM(spectuple, ii);
            pycbc_pybuffer pathbuf = { NULL }, valbuf = { NULL };
            rv = sd_convert_spec(cur, specs + ii, &pathbuf, &valbuf);
            PYCBC_PYBUF_RELEASE(&pathbuf);
            PYCBC_PYBUF_RELEASE(&valbuf);
            if (rv != 0) {
                break;
            }
        }
    }

    if (rv == 0) {
        err = lcb_subdoc3(self->instance, mres, cmd);
        if (err == LCB_SUCCESS) {
            PyDict_SetItem((PyObject*)mres, key, (PyObject*)newitm);
            pycbc_assert(Py_REFCNT(newitm) == 2);
        }
    }

    free(specs);
    Py_DECREF(newitm);

    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        return -1;
    } else if (rv != 0) {
        return -1;
    }
    return 0;
}
