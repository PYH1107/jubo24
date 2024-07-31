# Import of built in packages
import re
import json
import pytz
from collections import OrderedDict
from datetime import datetime, timedelta

# Import of third party packages
import pandas as pd


class TransformDataWorker:
    """
    This worker will capture the object detected by the DataHub watcher and
    make transformation to it as a preparation process before writing it to
    DataHub's database.
    """

    def __init__(self, single_watcher_file: json):
        if not isinstance(single_watcher_file, dict):
            raise TypeError(
                f"single_watcher_file need to be dict, get {type(single_watcher_file)}")
        single_watcher_file = OrderedDict(single_watcher_file)
        self.msg = json.dumps(single_watcher_file)
        self._id = single_watcher_file["documentKey"]["_id"]["$oid"]
        self.db = single_watcher_file["ns"]["db"]
        self.collection = single_watcher_file["ns"]["coll"]
        self.action = single_watcher_file["operationType"]
        self.time = datetime.fromtimestamp(
            single_watcher_file["clusterTime"]["$timestamp"]["t"], pytz.timezone("Asia/Taipei"))
        if self.action == "insert":
            self.data = trans_column_value(
                single_watcher_file["fullDocument"]
            )
            self.data_str, self.columns, self.values, self.dtypes = trans_data(
                self.data)
        elif self.action == "update":
            self.data = trans_column_value(
                single_watcher_file["updateDescription"]["updatedFields"]
            )
            self.data_str, self.columns, self.values, self.dtypes = trans_data(
                self.data)
            remove_fields = single_watcher_file[
                "updateDescription"]["removedFields"]
            if remove_fields:
                for field in remove_fields:
                    self.data[field] = None
        elif self.action == "delete":
            self.data = None
        else:
            raise KeyError("Unknown action type.")

    def __repr__(self):
        """
        Return the job task detected by the watcher which will be transformed
        in this worker.
        """
        return (f"Transform the {self.action} of {self._id} to {self.db}"
                f".{self.collection} at {self.time}")

    def __str__(self):
        """
        Return the content which this worker holds after transformation.
        """
        return self.data.__repr__()

    def to_sql(self):
        """
        This function is used to generate the sql query statement for writing
        the data inside this class to a sql database. To avoid sql injection
        the generated sql statement will be using placeholder %s method,
        so this function will have two return values.
        """
        if self.action == "insert":
            # ['col1', 'col2', ...] -> ['"col1"','"col2"', ...]
            k_string_list = [f'"{k}"' for k in self.columns]

            sql_string = (
                f'INSERT INTO "{self.collection}" ('
                f'{", ".join(k_string_list)}) '
                f'VALUES ({", ".join(["%s"] * self.data.__len__())})'
            )
            sql_val = list(self.values)
        elif self.action == "update":
            # ['col1', 'col2', ...] -> ['"col1"','"col2"', ...]
            k_string_list = [f'"{k}"' for k in self.columns]

            sql_string = (
                f'UPDATE "{self.collection}" '
                f'SET {" = %s, ".join(k_string_list)} = %s '
                f'WHERE "_id" = %s'
            )

            sql_val = list(self.values)
            sql_val.append(self._id)
        elif self.action == "delete":
            sql_string = (
                f'DELETE FROM "{self.collection}" '
                f'WHERE "_id" = %s'
            )
            sql_val = [self._id]
        else:
            raise TypeError("Unrecognized sql action type, cannot generate "
                            "the sql query.")

        return sql_string, sql_val


def trans_data(data):
    data = data.copy()
    for k, v in data.items():
        if isinstance(v, dict) or isinstance(v, list):
            data[k] = json.dumps(v)
    # data, columns, values, dtypes
    return data, list(data.keys()), list(data.values()), [type(v) for v in data.values()]


def trans_date_string_to_date(doc):
    """
    This function will check the input doc and tries to transfer strings
    to datetime object if the string is transferable to datetime.
    """
    if isinstance(doc, str):
        try:
            doc = datetime.strptime(doc, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            pass
    elif isinstance(doc, dict):
        for k, v in doc.items():
            doc[k] = trans_date_string_to_date(v)
    elif isinstance(doc, list):
        for i, v in enumerate(doc):
            doc[i] = trans_date_string_to_date(v)
    else:
        doc = doc
    return doc


def _is_mongo_dtype(d: dict):
    mongo_dtype = False
    if isinstance(d, dict):
        if len(d) == 1:
            k = list(d.keys())[0]
            if k.startswith("$"):
                mongo_dtype = True
    return mongo_dtype


def _trans_mongo_dtype(d: dict):
    k, v = list(d.items())[0]
    if k == "$oid":
        k = v
    elif k == "$date":
        if isinstance(v, dict):
            k = (datetime(1970, 1, 1) +
                 timedelta(seconds=(int(v["$numberLong"]) / 1000)))
        elif isinstance(v, int):
            k = (datetime(1970, 1, 1) +
                 timedelta(seconds=(int(v) / 1000)))
        else:
            k = pd.to_datetime(v)
        k = k.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        raise TypeError("Unrecognized MongoDB data type.")
    return k


def trans_column_value(col):
    if isinstance(col, dict):
        if _is_mongo_dtype(col):
            col = _trans_mongo_dtype(col)
        else:
            for k, v in col.items():
                col[k] = trans_column_value(v)
    elif isinstance(col, list):
        for index, item in enumerate(col):
            if _is_mongo_dtype(item):
                col[index] = _trans_mongo_dtype(item)
            else:
                col[index] = trans_column_value(item)
    elif isinstance(col, (str, int, float, type(None))):
        col = str(col)
        # replace some char made uploading to postgres error
        if '"' in col:
            col = col.replace('"', '')
    # elif isinstance(col, type(None)):
    #     col = str(col)
    else:
        raise TypeError(F"Type {type(col)}, is unable to transform.")

    return col
