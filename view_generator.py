import os
import csv
import re

pattern_to_replc = re.compile('[^A-Za-z0-9_]')

class DataSource:
    def __init__(self):
        self.key = None
        self.impl_name = []
        self.is_db_link = False
        self.db_link_name = None

    def checkIsDS(self, source) :
        return source.lower() in self.impl_name

    def addDS(self, source) :
        self.impl_name.append(source.lower())

class TableSource:
    def __init__(self):
        self.table_name = None
        self.table_abbrv = None
        self.table_source = None
        self.table_schema = None # not implemented yet
        # join info if any
        self.table_pk = None

    def print_to_str(self) :
        if self.table_source:
            if self.table_source.is_db_link :
                return self.table_source.key + "." + self.table_name + '@' + self.table_source.db_link_name + ' ' + self.table_abbrv
            else:
                return self.table_source.key + '.' + self.table_name + ' ' + self.table_abbrv
        return self.table_name + ' ' + self.table_abbrv

    def __str__(self):
        return self.print_to_str()

    def __repr__(self):
        return self.print_to_str()

class DataCol:
    def __init__(self):
        self.column_name = None
        self.column_name_as = None
        self.source = None
        self.table = None
        self.data_type = None
        self.null_value = None
    
    def get_column_name_as_norm(self):
        return re.sub(pattern_to_replc, '', self.column_name_as.strip().upper().replace(" ", "_"))
        # return self.column_name_as.upper().replace(" ", "_")

def getDS(key, source_list):
    result = None
    for source in source_list:
        if source.checkIsDS(key):
            result = source
            break
    return result

def getTableAbbrv(data, source_list):
    tl = {}
    abbrv_taken = {}
    for d in data:
        source_lower = d.source.lower()
        ds = None
        if 'tbd' in source_lower:
            #TBD, need to ask
            pass
        elif 'duplicate' in source_lower:
            # no need to add
            continue
        else:
            ds = getDS(d.source, source_list)


        t = d.table
        if t and t.lower() not in tl:
            ts = TableSource()
            ts.table_name = t
            ts.table_source = ds
            abbrv_tmp = t.lower()[:1]
            ts.table_abbrv = abbrv_tmp
            if abbrv_tmp not in abbrv_taken:
                abbrv_taken[abbrv_tmp] = 0
            else:
                ts.table_abbrv = abbrv_tmp + str(abbrv_taken[abbrv_tmp] + 1)
                abbrv_taken[abbrv_tmp] = abbrv_taken[abbrv_tmp] + 1
            tl[t.lower()] = ts
    return tl

def print_query(report_name, data, source_list, table_abbrv_map):    
    result_header = "CREATE VIEW VW_MI_" + report_name + " ("
    result_cols = "\tSELECT "
    for d in data:
        # check source
        source_lower = d.source.lower()
        ds = None
        if 'tbd' in source_lower:
            #TBD, need to ask
            pass
        elif 'duplicate' in source_lower:
            # no need to add
            continue
        else:
            ds = getDS(d.source, source_list)
        
        result_header += d.get_column_name_as_norm() + ", "
        if d.table and ds is not None:
            ds_table_abbrv = table_abbrv_map[d.table.lower()]

            result_cols += "\t"+ds_table_abbrv.table_abbrv+"."+d.column_name+" AS " + d.get_column_name_as_norm()+",\n"
        elif 'calculated' in source_lower:
            result_cols += "\t'CALCULATED_FIELD' AS " + d.get_column_name_as_norm()+",\n"
        elif 'manual' in source_lower:
            result_cols += "\t'BLANK - FILL IN' AS " + d.get_column_name_as_norm()+",\n"
        else:
            result_cols += "\t'N/A' AS " + d.get_column_name_as_norm()+",\n"
    
    result_header = result_header[:-2] + ") AS \n"
    result_cols = result_cols[:-2] + "\n"

    result_from = "\tFROM "
    for key in table_abbrv_map:
        result_from += "\t"+table_abbrv_map[key].print_to_str() + ",\n"

    result_from = result_from[:-2] + ";"

    result_str = result_header + result_cols + result_from

    return result_str

def read_data(csv_file_path):
    report_mapping = {}
    with open(csv_file_path, 'rb') as f :
        reader = csv.reader(f)
        current_report_name = None
        current_report = None
        for row in reader:
            if row[0] != '':
                # save prev report
                if current_report_name:
                    report_mapping[current_report_name] = current_report
                # new report
                current_report_name = row[0]
                current_report = []
                # print(current_report_name)
                continue
            elif row[0] == '' and row[1] == '':
                # break before new report
                continue
            else:
                d1 = DataCol()
                d1.column_name_as = row[1].strip()
                d1.source = row[3].strip()
                d1.column_name = row[4].strip()
                d1.table = row[5].strip()
                current_report.append(d1)
        if current_report_name:
            report_mapping[current_report_name] = current_report
    return report_mapping

def write_sql_file(view_table_name, output_dir, sql) :
    file_path = os.path.join(output_dir, view_table_name + ".sql")
    print('writing to ' + file_path)
    with open(file_path, "w") as f :
        f.write(sql)

if __name__ == "__main__":
    input_dir = "input"
    output_dir = "output"
    
    source_list = []
    cm_source = DataSource()
    cm_source.key = "CMDEV"
    cm_source.is_db_link = False
    cm_source.addDS('CM')
    cm_source.addDS('System generated')
    source_list.append(cm_source)

    cm_source1 = DataSource()
    cm_source1.key = "ODSDEV"
    cm_source1.is_db_link = True
    cm_source1.db_link_name = "ABC_LINK"
    cm_source1.addDS('LifeAsia')
    source_list.append(cm_source1)

    # report_name = "test123"

    # view_table_name = "VW_MI_" + report_name

    # data = []
    # d1 = DataCol()
    # d1.column_name = 'NO'
    # d1.column_name_as = 'No'
    # d1.source = 'CM'
    # d1.table = 'ABC'

    # d2 = DataCol()
    # d2.column_name = 'NAME'
    # d2.column_name_as = 'Name 1'
    # d2.source = 'CM'
    # d2.table = 'BCD'

    # d3 = DataCol()
    # d3.column_name = 'DATA'
    # d3.column_name_as = 'Data A'
    # d3.source = 'LifeAsia'
    # d3.table = 'AD'

    # data.append(d1)
    # data.append(d2)
    # data.append(d3)    

    # table_abbrv = getTableAbbrv(data, source_list)
    # print(table_abbrv)

    # sql = print_query(report_name, data, source_list, table_abbrv)
    # write_sql_file(view_table_name, output_dir, sql)

    report_mapping = {}
    onlyfiles = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
    for f in onlyfiles:
        fpath = os.path.join(input_dir, f)
        print('====='+fpath+'=====')
        res_mapping = read_data(fpath)
        report_mapping.update(res_mapping)
        print('=====')

    for key, data_list in report_mapping.iteritems():
        print(key)
        table_abbrv = getTableAbbrv(data_list, source_list)
        report_name = key.upper().replace(" ", "_")
        view_table_name = "VW_MI_" + report_name
        sql = print_query(report_name, data_list, source_list, table_abbrv)
        write_sql_file(view_table_name, output_dir, sql)
