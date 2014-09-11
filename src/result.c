/**
 *     Copyright 2013 Couchbase, Inc.
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

#include "pycbc.h"
#include "structmember.h"

static PyObject *
Result_success(pycbc_Result *self, void *closure)
{
    (void)closure;
    return PyBool_FromLong(self->rc == LCB_SUCCESS);
}

static PyObject *
Result_repr(pycbc_Result *self)
{
    return PyObject_CallFunction(pycbc_helpers.result_reprfunc, "O", self);
}


static PyObject *
Result_errstr(pycbc_Result *self, void *closure)
{
    (void)closure;
    return pycbc_lcb_errstr(NULL, self->rc);
}


static PyObject *
Result_value(pycbc_Result *self, void *closure)
{
    (void)closure;

    if (!self->value) {
        Py_INCREF(Py_None);
        return Py_None;
    }
    Py_INCREF(self->value);
    return self->value;
}

static struct PyMemberDef Result_TABLE_members[] = {
        { "rc",
                T_INT, offsetof(pycbc_Result, rc),
                READONLY,
                PyDoc_STR("libcouchbase error code")
        },
        { "key", T_OBJECT_EX, offsetof(pycbc_Result, key),
                READONLY,
                PyDoc_STR("Key for the operation")
        },
        { "cas",
                T_ULONGLONG, offsetof(pycbc_Result, cas),
                READONLY, PyDoc_STR("CAS For the key")
        },
        { "flags",
                T_UINT, offsetof(pycbc_Result, flags),
                READONLY, PyDoc_STR("Flags for the value")
        },

        { NULL }
};

static struct PyGetSetDef Result_TABLE_getset[] = {
        { "success",
                (getter)Result_success,
                NULL,
                PyDoc_STR("Determine whether operation succeeded or not")
        },
        { "value",
                (getter)Result_value,
                NULL,
                PyDoc_STR("Value for the operation")
        },
        { "errstr",
                (getter)Result_errstr,
                NULL,
                PyDoc_STR("Returns a textual representation of the error")
        },
        { NULL }
};

PyTypeObject pycbc_ResultType = { PYCBC_POBJ_HEAD_INIT(NULL) 0 };

static PyMethodDef Result_TABLE_methods[] = {
        { NULL }
};

static void
Result_dealloc(pycbc_Result *self)
{
    Py_XDECREF(self->key);
    Py_XDECREF(self->value);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

void
pycbc_Result_dealloc(pycbc_Result *self)
{
    Result_dealloc(self);
}

int
pycbc_ResultType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ResultType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "Result";
    p->tp_doc = PyDoc_STR(
            "The standard return type for Couchbase operations.\n"
            "\n"
            "This is a lightweight object and may be subclassed by other\n"
            "operations which may required additional fields.");

    p->tp_new = PyType_GenericNew;
    p->tp_dealloc = (destructor)Result_dealloc;
    p->tp_basicsize = sizeof(pycbc_Result);
    p->tp_members = Result_TABLE_members;
    p->tp_methods = Result_TABLE_methods;
    p->tp_getset = Result_TABLE_getset;
    p->tp_repr = (reprfunc)Result_repr;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;

    return pycbc_ResultType_ready(p, PYCBC_RESULT_BASEFLDS);
}

PyObject *
pycbc_result_new(pycbc_Bucket *parent)
{
    PyObject *obj = PyObject_CallFunction(
        (PyObject*) &pycbc_ResultType, NULL, NULL);
    (void)parent;
    return obj;
}

int
pycbc_ResultType_ready(PyTypeObject *p, int flags)
{
    int rv;
    PyObject *flags_o;

    rv = PyType_Ready(p);
    if (rv) {
        return rv;
    }

    flags_o = pycbc_IntFromUL(flags);
    PyDict_SetItemString(p->tp_dict, PYCBC_RESPROPS_NAME, flags_o);
    Py_DECREF(flags_o);

    return rv;
}

static void
Item_dealloc(pycbc_Item *self)
{
    Py_XDECREF(self->vdict);
    Result_dealloc((pycbc_Result*)self);
}

static int
Item__init__(pycbc_Item *item, PyObject *args, PyObject *kwargs)
{
    if (pycbc_ResultType.tp_init((PyObject *)item, args, kwargs) != 0) {
        return -1;
    }

    if (!item->vdict) {
        item->vdict = PyDict_New();
    }
    return 0;
}


/**
 * We need to re-define all these fields again and indicate their permissions
 * as being writable
 */
static PyMemberDef Item_TABLE_members[] = {
        { "__dict__",
            T_OBJECT_EX, offsetof(pycbc_Item, vdict), READONLY
        },

        { "value",
            T_OBJECT_EX, offsetof(pycbc_Item, value), 0,
            PyDoc_STR("The value of the Item.\n\n"
                    "For storage operations, this value is read. For retrieval\n"
                    "operations, this field is set\n")
        },

        { "cas",
            T_ULONGLONG, offsetof(pycbc_Item, cas), 0,
            PyDoc_STR("The CAS of the Item.\n\n"
                    "This field is always updated. On storage operations,\n"
                    "this field (if not ``0``) is used as the CAS for the\n"
                    "current operation. If the CAS on the server does not\n"
                    "match the value in this property, the operation will\n"
                    "fail.\n"
                    "For retrieval operations, this field is simply\n"
                    "set with the current CAS of the Item\n")
        },

        { "flags",
            T_UINT, offsetof(pycbc_Item, flags), 0,
            PyDoc_STR("The flags (format) of the Item.\n\n"
                    "This field is set\n"
                    "During a retrieval operation. It is not read for a \n"
                    "storage operation\n")
        },

        {"key",
            T_OBJECT_EX, offsetof(pycbc_Item, key), 0,
            PyDoc_STR("This is the key for the Item. It *must* be set\n"
                    "before passing this item along in any operation\n")
        },

        { NULL }
};


PyTypeObject pycbc_ItemType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

int pycbc_ItemType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_ItemType;
    *ptr = (PyObject *)p;
    if (p->tp_name) {
        return 0;
    }
    p->tp_name = "Item";
    p->tp_doc = PyDoc_STR(
            "Subclass of a :class:`~couchbase.result.Result`.\n"
            "This can contain user-defined fields\n"
            "This can also be used as an item in either a\n"
            ":class:`ItemOptionDict` or a :class:`ItemSequence` object which\n"
            "can then be passed along to one of the ``_multi`` operations\n"
            "\n");
    p->tp_basicsize = sizeof(pycbc_Item);
    p->tp_base = &pycbc_ResultType;
    p->tp_members = Item_TABLE_members;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_init = (initproc)Item__init__;
    p->tp_dealloc = (destructor)Item_dealloc;
    p->tp_dictoffset = offsetof(pycbc_Item, vdict);
    return pycbc_ResultType_ready(p, PYCBC_VALRESULT_BASEFLDS);
}


pycbc_Item *
pycbc_item_new(pycbc_Bucket *parent)
{
    (void)parent;
    return (pycbc_Item *)
            PyObject_CallFunction((PyObject*)&pycbc_ItemType, NULL, NULL);
}
